import aiofiles
import aiofiles.os
import aws_encryption_sdk
import boto3
import contextlib
import hashlib

from aws_cryptographic_material_providers.mpl import AwsCryptographicMaterialProviders
from aws_cryptographic_material_providers.mpl.config import MaterialProvidersConfig
from aws_cryptographic_material_providers.mpl.models import (
    AesWrappingAlg,
    CreateRawAesKeyringInput,
)
from aws_cryptographic_material_providers.mpl.references import IKeyring
from aws_encryption_sdk import CommitmentPolicy, EncryptionSDKClient
from aws_encryption_sdk.exceptions import AWSEncryptionSDKClientError
from botocore.exceptions import ClientError
from dataclasses import dataclass
from mypy_boto3_s3.client import S3Client
from pathlib import Path

from base.models.context import NdService
from base.strings.auth import ServiceId

from knowledge.config import KnowledgeConfig
from knowledge.models.exceptions import KnowledgeError

SVC_STORAGE = ServiceId.decode("svc-storage")

KEY_NAMESPACE = "KnowledgeStorageS3"
KEY_NAME = "DefaultEncryptionKey"

OBJECT_LIST_THRESHOLD: int = 100
"""
How many resources should be returned in a single call to object_list, before we
stop expanding the sub-directories and return them as dynamic collections.
"""


@dataclass(kw_only=True)
class ObjectList:
    prefixes: list[str]
    objects: list[str]


@dataclass(kw_only=True)
class SvcStorage(NdService):
    service_id: ServiceId = SVC_STORAGE

    @staticmethod
    def initialize() -> "SvcStorage":
        if (
            KnowledgeConfig.storage.aws_access_key
            and KnowledgeConfig.storage.aws_secret_key
            and KnowledgeConfig.storage.s3_name
            and KnowledgeConfig.storage.s3_region
        ):
            return SvcStorageS3.initialize(
                aws_access_key=KnowledgeConfig.storage.aws_access_key,
                aws_secret_key=KnowledgeConfig.storage.aws_secret_key,
                bucket_name=KnowledgeConfig.storage.s3_name,
                bucket_region=KnowledgeConfig.storage.s3_region,
                encryption_key=KnowledgeConfig.storage.encryption_key,
            )
        elif KnowledgeConfig.is_kubernetes():
            raise ValueError("Must use StorageS3 in Kubernetes")
        elif KnowledgeConfig.debug.storage_root:
            return SvcStorageLocal(
                root_dir=Path(KnowledgeConfig.debug.storage_root),
            )
        else:
            return SvcStorageStub.initialize()

    async def object_delete(self, path: str, ext: str) -> bool:
        raise NotImplementedError("Subclasses must implement Storage.object_delete")

    async def object_exists(self, path: str, ext: str) -> bool:
        raise NotImplementedError("Subclasses must implement Storage.object_exists")

    async def object_get(self, path: str, ext: str) -> str | None:
        """
        Read the text of the object for the specified path in Storage.
        Returns `None` if the object does not exist.

        NOTE: This method does not validate permissions.  The caller MUST check
        that the client is allowed to read the resource (or cache).
        """
        raise NotImplementedError("Subclasses must implement Storage.object_get")

    async def object_list(
        self,
        prefix: str,
        ext: str,
        threshold: int = OBJECT_LIST_THRESHOLD,
    ) -> ObjectList:
        """
        Read the list of objects whose path is equal to or under the specified
        prefix in storage.  Returns an empty list when no such objects exist.

        NOTE: This method does not validate permissions.  The caller MUST filter
        the resources (or caches) that the client is allowed to view.
        """
        prefixes: list[str] = [prefix]
        objects: list[str] = []

        if await self.object_exists(prefix, ext):
            objects.append(prefix)

        while prefixes and len(objects) < OBJECT_LIST_THRESHOLD:
            new_prefixes: list[str] = []
            for prefix_key in prefixes:
                children = await self._object_list_once(prefix_key, ext)
                objects.extend(children.objects)
                new_prefixes.extend(children.prefixes)
            prefixes = new_prefixes

        return ObjectList(prefixes=sorted(prefixes), objects=sorted(objects))

    async def _object_list_once(self, prefix: str, ext: str) -> ObjectList:
        raise NotImplementedError("Subclasses must implement Storage._object_list_once")

    async def object_set(self, path: str, ext: str, content: str) -> None:
        """
        Write the text to the object for the specified mode and URI in Storage.

        NOTE: This method does not validate permissions.  The caller MUST check
        that the client is allowed to write to the resource (or cache) BEFORE
        invoking it.
        """
        raise NotImplementedError("Subclasses must implement Storage.object_set")


##
## Stub
##


@dataclass(kw_only=True)
class SvcStorageStub(SvcStorage):
    items: dict[str, str]

    @staticmethod
    def initialize(
        items: list[tuple[str, str]] | None = None,
    ) -> "SvcStorageStub":
        return SvcStorageStub(
            items=dict(items) if items else {},
        )

    async def object_delete(self, path: str, ext: str) -> bool:
        if f"{path}{ext}" in self.items:
            del self.items[f"{path}{ext}"]
            return True
        else:
            return False

    async def object_exists(self, path: str, ext: str) -> bool:
        return f"{path}{ext}" in self.items

    async def object_get(self, path: str, ext: str) -> str | None:
        return self.items.get(f"{path}{ext}")

    async def _object_list_once(self, prefix: str, ext: str) -> ObjectList:
        objects: list[str] = []
        prefixes: set[str] = set()

        for key in self.items:
            if key.startswith(f"{prefix}/"):
                relative_path = key.removeprefix(f"{prefix}/")
                if "/" in relative_path:
                    prefixes.add(relative_path.split("/", 1)[0])
                elif key.endswith(ext):
                    objects.append(key.removesuffix(ext))

        return ObjectList(
            prefixes=sorted(f"{prefix}/{folder_name}" for folder_name in prefixes),
            objects=objects,
        )

    async def object_set(self, path: str, ext: str, content: str) -> None:
        self.items[f"{path}{ext}"] = content


##
## Local Filesystem
##


@dataclass(kw_only=True)
class SvcStorageLocal(SvcStorage):
    root_dir: Path

    async def object_delete(self, path: str, ext: str) -> bool:
        try:
            file_path = self.root_dir / f"{path}{ext}"
            if not file_path.exists():
                return False

            file_path.unlink(missing_ok=True)
            return True
        except (ValueError, OSError) as exc:
            raise KnowledgeError("Unexpected error: cannot delete object") from exc

    async def object_exists(self, path: str, ext: str) -> bool:
        file_path = self.root_dir / f"{path}{ext}"
        return file_path.is_file()

    async def object_get(self, path: str, ext: str) -> str | None:
        try:
            file_path = self.root_dir / f"{path}{ext}"
            if file_path.exists():
                async with aiofiles.open(file_path, "r") as f:
                    return await f.read()
            else:
                return None
        except (ValueError, OSError) as exc:
            raise KnowledgeError("Unexpected error: cannot get object") from exc

    async def _object_list_once(self, prefix: str, ext: str) -> ObjectList:
        try:
            folder_path = self.root_dir / prefix
            if not folder_path.is_dir():
                return ObjectList(prefixes=[], objects=[])

            prefixes: list[str] = []
            objects: list[str] = []
            for entry in await aiofiles.os.scandir(folder_path):
                if entry.is_dir(follow_symlinks=False):
                    prefixes.append(entry.name)
                elif entry.is_file(follow_symlinks=False) and entry.name.endswith(ext):
                    objects.append(entry.name.removesuffix(ext))

            return ObjectList(
                prefixes=sorted(prefixes),
                objects=sorted(objects),
            )
        except (ValueError, OSError) as exc:
            raise KnowledgeError("Unexpected error: cannot list objects") from exc

    async def object_set(self, path: str, ext: str, content: str) -> None:
        try:
            file_path = self.root_dir / f"{path}{ext}"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(file_path, "w") as f:
                await f.write(content)
        except (ValueError, OSError) as exc:
            raise KnowledgeError("Unexpected error: cannot set object") from exc


##
## S3
##


@dataclass(kw_only=True)
class SvcStorageS3(SvcStorage):
    bucket_name: str
    s3_client: S3Client
    encryption_enabled: bool
    encryption_client: EncryptionSDKClient | None
    encryption_keyring: IKeyring | None

    @staticmethod
    def initialize(  # pyright: ignore[reportIncompatibleMethodOverride]
        aws_access_key: str,
        aws_secret_key: str,
        bucket_name: str,
        bucket_region: str,
        encryption_key: str | None,
    ) -> "SvcStorageS3":
        s3_client = boto3.client(
            service_name="s3",
            region_name=bucket_region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )

        if encryption_key:
            encryption_enabled = True
            derived_key = hashlib.sha256(encryption_key.encode("utf-8")).digest()
            mat_prov: AwsCryptographicMaterialProviders = (
                AwsCryptographicMaterialProviders(config=MaterialProvidersConfig())
            )

            keyring_input: CreateRawAesKeyringInput = CreateRawAesKeyringInput(
                key_namespace=KEY_NAMESPACE,
                key_name=KEY_NAME,
                wrapping_key=derived_key,
                wrapping_alg=AesWrappingAlg.ALG_AES256_GCM_IV12_TAG16,
            )

            encryption_keyring = mat_prov.create_raw_aes_keyring(
                input=keyring_input,
            )

            encryption_client = aws_encryption_sdk.EncryptionSDKClient(
                commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT,
            )
        else:
            encryption_enabled = False
            encryption_client = None
            encryption_keyring = None

        return SvcStorageS3(
            bucket_name=bucket_name,
            s3_client=s3_client,
            encryption_enabled=encryption_enabled,
            encryption_client=encryption_client,
            encryption_keyring=encryption_keyring,
        )

    def _encrypt(self, data: bytes) -> bytes:
        """Encrypts data using the configured keyring."""
        if not self.encryption_enabled:
            return data

        try:
            assert self.encryption_client is not None
            assert self.encryption_keyring is not None
            encrypted_data, _header = self.encryption_client.encrypt(
                source=data,
                keyring=self.encryption_keyring,
            )
            return encrypted_data
        except AWSEncryptionSDKClientError as exc:
            raise KnowledgeError("Failed to encrypt data") from exc

    def _decrypt(self, encrypted_data: bytes) -> bytes:
        """Decrypts data using the configured keyring."""
        if not self.encryption_enabled:
            return encrypted_data

        try:
            assert self.encryption_client is not None
            assert self.encryption_keyring is not None
            decrypted_data, _header = self.encryption_client.decrypt(
                source=encrypted_data,
                keyring=self.encryption_keyring,
            )
            return decrypted_data
        except AWSEncryptionSDKClientError as exc:
            raise KnowledgeError("Failed to decrypt data") from exc

    def _try_decode_s3_object(self, s3_object_bytes: bytes) -> bytes:
        """Attempts to decrypt and decode S3 object bytes, falling back to raw UTF-8 decode."""
        try:
            return self._decrypt(s3_object_bytes)
        except KnowledgeError:
            return s3_object_bytes

    async def object_delete(self, path: str, ext: str) -> bool:
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=f"{path}{ext}",
            )
            return True
        except Exception:
            return False

    async def object_exists(self, path: str, ext: str) -> bool:
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=f"{path}{ext}",
            )
            return True
        except Exception:
            return False

    async def object_get(self, path: str, ext: str) -> str | None:
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"{path}{ext}",
            )
            if body := obj.get("Body"):
                s3_object_bytes = body.read()
                decrypted_bytes = self._try_decode_s3_object(s3_object_bytes)

                # If the existing data on S3 is not encrypted, then replace the
                # old object's body by the encrypted bytes.
                if self.encryption_enabled and s3_object_bytes is decrypted_bytes:
                    try:
                        self.s3_client.put_object(
                            Bucket=self.bucket_name,
                            Key=f"{path}{ext}",
                            Body=self._encrypt(decrypted_bytes),
                        )
                    except Exception as exc:
                        raise KnowledgeError("Cannot re-encrypt old S3 object") from exc

                return decrypted_bytes.decode("utf-8")
        except KnowledgeError:
            raise
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchKey":  # type: ignore
                return None
            else:
                raise KnowledgeError("Cannot read S3 object") from exc
        except Exception as exc:
            raise KnowledgeError("Cannot read S3 object") from exc

        # Should not be reached if object exists, but required for type checking.
        return None

    async def _object_list_once(self, prefix: str, ext: str) -> ObjectList:
        """
        TODO: Only list the immediate children of the prefix.
        """
        objects: list[str] = []
        prefixes: list[str] = []

        with contextlib.suppress(ClientError):
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=f"{prefix}{ext}",
            )
            objects.append(prefix)

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for result in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=f"{prefix}/",
                # NOTE: No Delimiter to list all children recursively.
            ):
                if contents := result.get("Contents"):
                    objects.extend(
                        object_key.removesuffix(ext)
                        for obj in contents
                        if (object_key := obj.get("Key")) and object_key.endswith(ext)
                    )
        except Exception as exc:
            raise KnowledgeError("Cannot list sources") from exc

        return ObjectList(
            prefixes=sorted(prefixes),
            objects=sorted(objects),
        )

    async def object_set(self, path: str, ext: str, content: str) -> None:
        try:
            encrypted_data = self._encrypt(content.encode("utf-8"))
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{path}{ext}",
                Body=encrypted_data,
            )
        except KnowledgeError:
            raise
        except Exception as exc:
            raise KnowledgeError("Cannot write source data") from exc
