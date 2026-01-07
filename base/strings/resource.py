import re

from pydantic_core import core_schema
from pydantic.annotated_handlers import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from typing import Generic, Literal, Self, TypeVar
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from base.core.strings import StructStr, ValidatedStr
from base.strings.data import MimeType
from base.strings.file import FileName, FilePath, REGEX_FILENAME
from base.utils.sorted_list import bisect_make

REGEX_REALM = r"[a-z][a-z0-9]+(?:-[a-z0-9]+)*"
REGEX_RESOURCE_URI = rf"ndk://{REGEX_REALM}(?:/{REGEX_FILENAME}){{2,}}"
REGEX_SUFFIX = rf"\$[a-z]+(?:/{REGEX_FILENAME})*"

REGEX_KNOWLEDGE_URI = rf"{REGEX_RESOURCE_URI}(?:/{REGEX_SUFFIX})?"
REGEX_SUFFIX_FULL_URI = rf"{REGEX_RESOURCE_URI}/{REGEX_SUFFIX}"
REGEX_SUFFIX_SELF_URI = rf"self://{REGEX_SUFFIX}"

REGEX_WEB_DOMAIN = r"[a-zA-Z0-9][a-zA-Z0-9\-.]+\.[a-zA-Z]{2,}"
REGEX_WEB_URL_CHAR = r"(?:[a-zA-Z0-9]|[!$&\(\)+,\-./:=@_~]|(?:%[0-9a-fA-F][0-9a-fA-F]))"
REGEX_WEB_URL = (
    rf"https?://{REGEX_WEB_DOMAIN}(?::\d+)?"
    rf"(?:/{REGEX_WEB_URL_CHAR}*)?"
    rf"(?:\?{REGEX_WEB_URL_CHAR}*)?"
    rf"(?:#{REGEX_WEB_URL_CHAR}*)?"
)

REGEX_EXTERNAL_URI = f"{REGEX_WEB_URL}"
REGEX_REFERENCE = f"{REGEX_KNOWLEDGE_URI}|{REGEX_EXTERNAL_URI}"


##
## Knowledge Realm
##


class Realm(ValidatedStr):
    """
    The "realm" of a resource is a prefix that tells Nandam who handles actions.
    Each realm corresponds to either:

    - A connector in Knowledge (e.g., "jira"), or
    - A service in Nandam Backend (e.g., "local").

    This allows:

    - Backend to route each action to the service that owns the resource; and
    - Knowledge to route each action to the connector that implements it.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["jira", "sharepoint", "www"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_REALM


##
## Knowledge Suffix
##


class KnowledgeSuffix(StructStr, frozen=True):
    path: list[FileName]

    @classmethod
    def variants(cls) -> list[type[Self]]:
        if cls is KnowledgeSuffix:
            subclasses = [*Affordance.__subclasses__(), *Observable.__subclasses__()]
        elif cls is Affordance:
            subclasses = Affordance.__subclasses__()
        elif cls is Observable:
            subclasses = Observable.__subclasses__()
        else:
            return [cls]
        return bisect_make(subclasses, key=lambda x: x.suffix_kind())  # type: ignore

    @classmethod
    def find_subclass_by_kind(cls, kind: str) -> "type[Self] | None":
        return next(
            (sub for sub in cls.variants() if sub.suffix_kind() == kind),
            None,
        )

    @classmethod
    def find_subclass_by_uri(cls, uri: str) -> "type[Self] | None":
        """
        NOTE: Accepts
        - Suffix strings: "$..."
        - `Affordance` strings: "self://$..."
        - `AffordanceUri` strings: "ndk://.../$..."
        """
        suffix: str | None = None
        if uri.startswith("$"):
            suffix = uri
        elif uri.count("/$") == 1:
            suffix = "$" + uri.split("/$", maxsplit=1)[1]

        if suffix:
            suffix_kind = suffix.split("/", maxsplit=1)[0][1:]
            return cls.find_subclass_by_kind(suffix_kind)
        else:
            return None

    ##
    ## Suffix representation
    ##

    @classmethod
    def suffix_kind(cls) -> str:
        raise NotImplementedError(
            "Subclasses must implement KnowledgeSuffix.suffix_kind"
        )

    @classmethod
    def parse_suffix(cls, suffix: str) -> Self:
        """
        Translate a suffix string into the relevant suffix subclass.
        Only runs after the regex matches successfully.
        """
        suffix_type = cls.find_subclass_by_uri(suffix)
        if not suffix_type or not re.fullmatch(
            suffix_type._suffix_regex(), suffix  # noqa: SLF001
        ):
            raise ValueError(f"invalid {cls.__name__} suffix, given '{suffix}'")

        return suffix_type(
            path=[
                FileName.decode_part(suffix_type, suffix, part)
                for part in suffix.split("/")[1:]
            ],
        )

    def as_suffix(self) -> str:
        return "/".join([f"${self.suffix_kind()}", *self.path])

    @classmethod
    def _suffix_regex(cls) -> str:
        return rf"\${cls.suffix_kind()}(/{REGEX_FILENAME})*"

    @classmethod
    def _suffix_examples(cls) -> list[str]:
        return [f"${cls.suffix_kind()}"]

    ##
    ## Self URI representation
    ##

    @classmethod
    def _parse(cls, v: str) -> Self:
        return cls.parse_suffix(v.removeprefix("self://"))

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            f"self://{example}"
            for suffix_type in cls.variants()
            for example in suffix_type._suffix_examples()  # noqa: SLF001
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        if cls is KnowledgeSuffix or cls is Affordance or cls is Observable:
            return REGEX_SUFFIX_SELF_URI
        else:
            return f"self://{cls._suffix_regex()}"

    def _serialize(self) -> str:
        return f"self://{self.as_suffix()}"


class Affordance(KnowledgeSuffix, frozen=True):
    """
    Each "affordance" of a resource is a unique identifier for a "perspective"
    that can be taken on the resource, which determines the actions that can be
    performed on it.

    For example: a Word document can be viewed as...

    - The docx "file" itself, which can be used by tools like Code Interpreter,
      where its "content" is a download URL (or its raw bytes, when small).

    - The "body" representation, which can be read by LLMs. Its content has been
      converted to Markdown and broken down into "chunk" and "media" observables
      that fit into their context window ("$body" acts as a table of contents).

    NOTE: Never instantiated directly, but instead, parsing returns a subclass.
    Therefore, all subclasses MUST define `type: Literal` with a default value,
    which is used as the prefix of the URI suffix.
    """


class Observable(KnowledgeSuffix, frozen=True):
    """
    An "observable" is a path within an affordance.

    For example, when a Word document is viewed as "$body", its content may be
    broken down into "chunks" and "media" parts, allowing clients to consult or
    cite specific parts using their URIs.

    NOTE: Not all affordances have observables: they are only useful when their
    internals can be meaningfully manipulated by AI agents on their own terms.
    """

    def affordance(self) -> Affordance:
        raise NotImplementedError("Subclasses must implement Observable.affordance")

    def root(self) -> "Observable":
        return self


##
## Reference
##


class Reference(StructStr, frozen=True):
    @classmethod
    def _parse(cls, v: str) -> "Reference":
        if v.startswith("ndk://"):
            return KnowledgeUri.decode(v)
        elif v.startswith("https://"):
            return WebUrl.decode(v)
        else:
            raise ValueError(f"invalid Reference: invalid scheme, got '{v}'")

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            *WebUrl._schema_examples(),  # noqa: SLF001
            *KnowledgeUri._schema_examples(),  # noqa: SLF001
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_REFERENCE

    def guess_filename(self, default_mime: MimeType | None = None) -> "FileName | None":
        return None


##
## Generic URI
##


class KnowledgeUri(Reference, frozen=True):
    """
    Knowledge URI is the base class for resource and affordance URIs, which give
    each resource (or "affordance" of this resource) a unique identifier.  It is
    NEVER instantiated directly, but instead, `decode` returns the subclass that
    corresponds to the input URI.

    You should think of these as IDs, instead of true URIs.  Their purpose is to
    provide a unified view on all resources and to deduplicate among equivalent
    ways of expressing its location (e.g., multiple web URLs to the same file).

    Resources also reference each other using these URIs, creating a knowledge
    graph that agents can traverse to find information.

    NOTE: Having these URIs guarantees neither that the resource or affordance
    exists, nor that the client is allowed to access it.
    """

    realm: Realm
    subrealm: FileName
    path: list[FileName]
    suffix: Affordance | Observable | None

    @classmethod
    def _parse(cls, v: str) -> Self:
        if "/$" not in v:
            return ResourceUri._parse(v)  # noqa: SLF001  # type: ignore

        resource_str, suffix_str = v.split("/$", maxsplit=1)
        resource_uri = ResourceUri._parse(resource_str)  # noqa: SLF001
        suffix_str = f"${suffix_str}"

        suffix_type = KnowledgeSuffix.find_subclass_by_uri(suffix_str)
        if not suffix_type:
            raise ValueError(f"invalid {cls.__name__}: unknown suffix, got '{v}'")

        try:
            suffix = suffix_type.parse_suffix(suffix_str)
        except ValueError as exc:
            error_message = f"invalid {cls.__name__}: invalid suffix, got '{v}'"
            raise ValueError(error_message) from exc

        if isinstance(suffix, Observable):
            return ObservableUri(  # type: ignore
                realm=resource_uri.realm,
                subrealm=resource_uri.subrealm,
                path=resource_uri.path,
                suffix=suffix,
            )
        else:
            assert isinstance(suffix, Affordance)
            return AffordanceUri(  # type: ignore
                realm=resource_uri.realm,
                subrealm=resource_uri.subrealm,
                path=resource_uri.path,
                suffix=suffix,
            )

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return bisect_make(
            [
                *ResourceUri._schema_examples(),  # noqa: SLF001
                *AffordanceUri._schema_examples(),  # noqa: SLF001
                *ObservableUri._schema_examples(),  # noqa: SLF001
                # "ndk://jira/issue/PROJ-123",
                # "ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx",
                # "ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx/$body",
                # "ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx/$chunk/01/02",
                # "ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx/$media/figure.png",
            ],
            key=lambda uri: str(uri),
        )

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_KNOWLEDGE_URI

    def _serialize(self) -> str:
        suffix = f"/{self.suffix.as_suffix()}" if self.suffix else ""
        resource_path = "/".join(self.path)
        return f"ndk://{self.realm}/{self.subrealm}/{resource_path}{suffix}"

    ##
    ## Utils
    ##

    def guess_filename(self, default_mime: MimeType | None = None) -> FileName:
        """
        By default, when a filename is required (e.g., uploading a file), we use
        the last component of the resource URI.  When the subclass can provide a
        better heuristic, this behaviour should be overridden.
        """
        return self.path[-1]

    def is_child_or(self, parent_or_self: "KnowledgeUri") -> bool:
        if self == parent_or_self:
            return True

        if isinstance(parent_or_self, ResourceUri):
            if isinstance(self, ResourceUri):
                return str(self).startswith(f"{parent_or_self}/")
            else:
                return self.resource_uri() == parent_or_self

        elif self.resource_uri() != parent_or_self.resource_uri():
            return False

        elif isinstance(parent_or_self, AffordanceUri) and isinstance(
            self, ObservableUri
        ):
            return self.suffix.affordance() == parent_or_self.suffix

        else:
            return False

    def resource_uri(self) -> "ResourceUri":
        if isinstance(self, ResourceUri):
            return self
        else:
            return ResourceUri(
                realm=self.realm,
                subrealm=self.subrealm,
                path=self.path,
            )


##
## Resource URI
##


class ResourceUri(KnowledgeUri, frozen=True):
    """
    The resource URI acts as the unique identifier for each resource manipulated
    by Backend and Knowledge, and all the services that depend on them.

    It includes the resource:

    - "realm": which connector should handle it;
    - "subrealm": where is the resource stored within the realm;
    - "path": what is its unique ID within this realm.

    The resource URI does NOT mention the "affordance" being used.  Its purpose
    is to load the resource's **metadata**, including its capabilities, whereas
    the affordance URIs of the resource are used to read its **content** in the
    format that fits the task at hand.
    """

    suffix: None = None

    @classmethod
    def _parse(cls, v: str) -> "ResourceUri":
        resource_path = v.removeprefix("ndk://")
        realm_str, subrealm_str, path_str = resource_path.split("/", maxsplit=2)
        if not (realm := Realm.try_decode(realm_str)):
            raise ValueError(f"invalid ResourceUri: invalid realm, got '{v}'")
        if not (subrealm := FileName.try_decode(subrealm_str)):
            raise ValueError(f"invalid ResourceUri: invalid subrealm, got '{v}'")
        if not (path := FilePath.try_decode(path_str)):
            raise ValueError(f"invalid ResourceUri: invalid path, got '{v}'")

        return ResourceUri(
            realm=realm,
            subrealm=subrealm,
            path=path.parts(),
            suffix=None,
        )

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "ndk://jira/issue/PROJ-123",
            "ndk://stub/-/dir/example",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_RESOURCE_URI

    @staticmethod
    def tmp() -> "ResourceUri":
        return ResourceUri.decode("ndk://tmp/-/-")

    def child(self, child_path: list[FileName]) -> "ResourceUri":
        return ResourceUri(
            realm=self.realm,
            subrealm=self.subrealm,
            path=[*self.path, *child_path],
        )

    def child_affordance[Aff: Affordance](self, suffix: Aff) -> "AffordanceUri[Aff]":
        return AffordanceUri(
            realm=self.realm,
            subrealm=self.subrealm,
            path=self.path,
            suffix=suffix,
        )

    def child_observable[Obs: Observable](self, suffix: Obs) -> "ObservableUri[Obs]":
        return ObservableUri(
            realm=self.realm,
            subrealm=self.subrealm,
            path=self.path,
            suffix=suffix,
        )

    def child_suffix(self, suffix: KnowledgeSuffix) -> "KnowledgeUri":
        if isinstance(suffix, Observable):
            return self.child_observable(suffix)
        elif isinstance(suffix, Affordance):
            return self.child_affordance(suffix)
        else:
            raise ValueError(f"invalid child KnowledgeSuffix: {suffix}")  # noqa: TRY004


##
## Affordance URI
## ---
## NOTE: We must pass `Generic[Aff]` AFTER `BaseModel` in the parent chain to
## populate `__pydantic_generic_metadata__` and access the type at runtime.
##


Aff = TypeVar("Aff", bound="Affordance")


class AffordanceUri(KnowledgeUri, Generic[Aff], frozen=True):  # noqa: UP046
    suffix: Aff

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        core_schema: core_schema.CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        schema = super().__get_pydantic_json_schema__(core_schema, handler)
        if aff_type := cls.suffix_type():
            aff_name = aff_type.__name__.removeprefix("Aff")
            schema.update(title=f"Resource{aff_name}Uri")
        return schema

    @classmethod
    def suffix_type(cls) -> "type[Aff] | None":
        if cls.__pydantic_generic_metadata__["args"]:
            affordance = cls.__pydantic_generic_metadata__["args"][0]
            if issubclass(affordance, Affordance) and affordance is not Affordance:
                return affordance  # type: ignore
        return None

    @classmethod
    def _parse(cls, v: str) -> "AffordanceUri[Aff]":
        assert v.count("/$") == 1
        resource_str, suffix_str = v.split("/$", maxsplit=1)
        resource_uri = ResourceUri._parse(resource_str)  # noqa: SLF001

        if aff_type := cls.suffix_type():
            error_type = f"AffordanceUri[{aff_type.__name__}]"
        else:
            aff_type = Affordance
            error_type = "AffordanceUri"

        try:
            suffix = aff_type.parse_suffix(f"${suffix_str}")
        except ValueError as exc:
            raise ValueError(f"invalid {error_type}: got '{v}'") from exc

        return AffordanceUri(
            realm=resource_uri.realm,
            subrealm=resource_uri.subrealm,
            path=resource_uri.path,
            suffix=suffix,  # type: ignore
        )

    @classmethod
    def _schema_examples(cls) -> list[str]:
        examples = [
            "ndk://stub/-/dir/example/$body",
            "ndk://stub/-/dir/example/$collection",
            "ndk://stub/-/dir/example/$file",
            "ndk://stub/-/dir/example/$file/figures/image.png",
            "ndk://stub/-/dir/example/$plain",
        ]
        if aff_type := cls.suffix_type():
            return [
                example
                for example in examples
                if AffordanceUri[aff_type].try_decode(example)
            ]
        else:
            return examples

    @classmethod
    def _schema_regex(cls) -> str:
        if aff_type := cls.suffix_type():
            return rf"{REGEX_RESOURCE_URI}/{aff_type._suffix_regex()}"  # noqa: SLF001
        else:
            return REGEX_SUFFIX_FULL_URI


##
## Observable URI
## ---
## NOTE: We must pass `Generic[Obs]` AFTER `BaseModel` in the parent chain to
## populate `__pydantic_generic_metadata__` and access the type at runtime.
##


Obs = TypeVar("Obs", bound="Observable")


class ObservableUri(KnowledgeUri, Generic[Obs], frozen=True):  # noqa: UP046
    suffix: Obs

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        core_schema: core_schema.CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        schema = super().__get_pydantic_json_schema__(core_schema, handler)
        if obs_type := cls.suffix_type():
            obs_name = obs_type.__name__.removeprefix("Aff")
            schema.update(title=f"Resource{obs_name}Uri")
        return schema

    @classmethod
    def suffix_type(cls) -> "type[Obs] | None":
        if cls.__pydantic_generic_metadata__["args"]:
            obs_type = cls.__pydantic_generic_metadata__["args"][0]
            if issubclass(obs_type, Observable) and obs_type is not Observable:
                return obs_type  # type: ignore
        return None

    @classmethod
    def _parse(cls, v: str) -> "ObservableUri[Obs]":
        assert v.count("/$") == 1
        resource_str, observable_str = v.split("/$", maxsplit=1)
        observable_suffix = f"${observable_str}"
        resource_uri = ResourceUri._parse(resource_str)  # noqa: SLF001

        if obs_type := cls.suffix_type():
            try:
                affordance = obs_type.parse_suffix(observable_suffix)
            except ValueError as exc:
                error_message = (
                    f"invalid ObservableUri[{obs_type.__name__}]: got '{v}': {exc}"
                )
                raise ValueError(error_message) from exc
        else:
            try:
                affordance = Observable.parse_suffix(observable_suffix)
            except ValueError as exc:
                error_message = f"invalid ObservableUri: got '{v}': {exc}"
                raise ValueError(error_message) from exc

        return ObservableUri(
            realm=resource_uri.realm,
            subrealm=resource_uri.subrealm,
            path=resource_uri.path,
            suffix=affordance,  # type: ignore
        )

    @classmethod
    def _schema_examples(cls) -> list[str]:
        if obs_type := cls.suffix_type():
            return [
                f"ndk://stub/-/dir/example/{suffix_str}"
                for suffix_str in obs_type._suffix_examples()  # noqa: SLF001
            ]
        else:
            return [
                f"ndk://stub/-/dir/example/{suffix_str}"
                for subclass in Observable.variants()
                for suffix_str in subclass._suffix_examples()  # noqa: SLF001
            ]

    @classmethod
    def _schema_regex(cls) -> str:
        if obs_type := cls.suffix_type():
            return rf"{REGEX_RESOURCE_URI}/{obs_type._suffix_regex()}"  # noqa: SLF001
        else:
            return REGEX_SUFFIX_FULL_URI

    def affordance_uri(self) -> "AffordanceUri":
        return AffordanceUri(
            realm=self.realm,
            subrealm=self.subrealm,
            path=self.path,
            suffix=self.suffix.affordance(),
        )

    def root_uri(self) -> "ObservableUri":
        suffix_root = self.suffix.root()
        if suffix_root == self.suffix:
            return self
        else:
            return ObservableUri(
                realm=self.realm,
                subrealm=self.subrealm,
                path=self.path,
                suffix=suffix_root,
            )


##
## Web
##


WEB_URL_PATH_PREFIXES: list[str] = [
    ":f:/r/",
    ":u:/r/",
]


class ExternalUri(Reference, frozen=True):
    @classmethod
    def _parse(cls, v: str) -> "ExternalUri":
        if v.startswith("https://"):
            return WebUrl.decode(v)
        else:
            raise ValueError(f"invalid ExternalUri: invalid scheme, got '{v}'")

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            *WebUrl._schema_examples(),  # noqa: SLF001
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_EXTERNAL_URI


class WebUrl(ExternalUri, frozen=True):
    """
    An arbitrary web URL (HTTPS-only) that...
    - Knowledge may translate into a source URL;
    - May be used as the `citation_url` or `download_url` of a Source.

    NOTE: Not all valid URLs are supported.  Disallowed chars: `"*<>[\\]"`.
    """

    scheme: Literal["https"] = "https"
    domain: str
    port: int
    path: str
    path_prefix: str | None
    query_path: str | None
    query: list[tuple[str, str]]
    fragment: str

    @classmethod
    def _parse(cls, v: str) -> "WebUrl":
        parsed = urlparse(v)
        netloc = parsed.netloc.lower()
        if ":" in netloc:
            domain, port_str = netloc.split(":", maxsplit=1)
            if not port_str.isdigit():
                raise ValueError(f"invalid WebUrl: bad port, got '{v}'")
            port = int(port_str)
        else:
            domain = netloc
            port = 443

        # Special case for TestRail, whose GET parameters have the form:
        # "/index.php?{query_path}&{query_params}"
        query_params = parsed.query
        query_path = None
        if re.fullmatch(rf"(?:/{REGEX_FILENAME})+/?", parsed.query):
            query_params = ""
            query_path = parsed.query
        elif re.match(rf"^(?:/{REGEX_FILENAME})+/?&", parsed.query):
            query_path, query_params = query_params.split("&", maxsplit=1)

        path = parsed.path.removeprefix("/")
        path_prefix = None
        for prefix in WEB_URL_PATH_PREFIXES:
            if path.startswith(prefix):
                path_prefix = prefix
                path = path.removeprefix(prefix)
                break

        return WebUrl(
            scheme="https",
            domain=domain,
            port=port,
            path=path,
            path_prefix=path_prefix,
            query_path=query_path,
            query=parse_qsl(query_params),
            fragment=parsed.fragment,
        )

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "https://example.com",
            "https://example.com/mypage.html?queryParam=42#fragment",
            "https://mycompany.atlassian.net/browse/PROJ-123",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_WEB_URL

    def _serialize(self) -> str:
        url_path = f"{self.path_prefix or ''}{self.path}"
        url_parts = (
            "https",
            self.domain if self.port in (80, 443) else f"{self.domain}:{self.port}",
            f"/{url_path}" if url_path else "",
            "",
            urlencode(self.query),
            self.fragment,
        )
        url = urlunparse(url_parts)

        if not self.query_path:
            return url

        return (
            f"{url}?{self.query_path}"
            if not self.query
            else url.replace("?", f"?{self.query_path}&")
        )

    ##
    ## Manipulation
    ##

    def clean(self) -> "WebUrl":
        return WebUrl(
            domain=self.domain,
            port=self.port,
            path=self.path,
            path_prefix=self.path_prefix,
            query_path=self.query_path,
            query=sorted(self.query),
            fragment=self.fragment,
        )

    def get_query(self, param: str) -> str | None:
        for key, value in self.query:
            if key == param:
                return value
        return None

    def guess_filename(self, default_mime: MimeType | None = None) -> "FileName | None":
        """
        Infer a default filename from the last component of the URL path, which
        should be overridden by `FileName.from_http_headers`.

        NOTE: If there is no extension, then assume that it is a web page and
        append the ".html" extension.  In such situations, a "content-disposition"
        """
        # If there no extension, assume it's a web page; it should be overriden
        # by the Content-Disposition header when that's incorrect
        last_component = self.path.removesuffix("/").rsplit("/", maxsplit=1)[-1]
        if last_component and (filename := FileName.try_normalize(last_component)):
            if (
                "." not in filename
                and default_mime
                and (default_ext := default_mime.guess_extension())
            ):
                return filename.with_ext(default_ext)
            else:
                return filename
        else:
            return None

    def try_join_href(self, link_href: str) -> "WebUrl | None":
        """
        Given an `href` on the current page, build the corresponding `WebUrl`.
        The resulting URL is guaranteed to be well-formed, but not to be valid.
        - Full URLs are returned as-is;
        - Absolute paths are returned on the same domain;
        - Relative paths are joined to the current one.
        """
        return WebUrl.try_decode(urljoin(str(self), link_href))
