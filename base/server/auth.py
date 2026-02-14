import base64
import json
import jwt
import logging
import time

from functools import cache
from pydantic import BaseModel, PrivateAttr
from typing import Any, Literal

from base.config import BaseConfig
from base.core.exceptions import AuthorizationError
from base.core.values import parse_yaml_as
from base.strings.auth import (
    Release,
    RequestId,
    UserHandle,
    UserId,
    parse_basic_credentials,
)
from base.strings.scope import (
    Scope,
    ScopeInternal,
    ScopeMsGroup,
    ScopePersonal,
    ScopePrivate,
)
from base.utils.sorted_list import bisect_make

logger = logging.getLogger(__name__)


##
## Configuration
##


def unittest_configure(
    *,
    auth_internal_secret: bool = False,
) -> None:
    if auth_internal_secret:
        BaseConfig.auth.internal_secret = (
            "01234567890abcdef01234567890abcdef01234567890abcdef01234567890ab"  # noqa: S105
        )
    else:
        BaseConfig.auth.internal_secret = None


class AuthClientConfig(BaseModel, frozen=True):
    release: Release
    """
    Expected username in the 'Basic' authorization header, which corresponds to
    the `release` of the client service.
    """
    secret: str
    """
    Environment variable that stores the expected password in the 'Basic'
    authorization header.
    """
    supports_keycloak: Literal["allow", "ignore", "required", "trusted"] = "allow"
    """
    Whether a Keycloak token is required to authenticate users on this client.
    When set to "unnecessary", the "x-user-id" header acts as authentication.
    """
    supports_internal: Literal["always", "never"] = "always"
    """
    Whether `ScopeInternal` can be instantiated for this client.
    """
    supports_msgroup: Literal["keycloak", "never"] = "never"
    """
    Whether `ScopeMsGroup` can be instantiated for this client.
    NOTE: This only makes sense for first-class clients such as Microsoft Teams.
    """
    supports_personal: Literal["keycloak", "never"] = "never"
    """
    Whether `ScopePersonal` can be instantiated for this client.
    """
    supports_private: Literal["always", "keycloak", "never"] = "keycloak"
    """
    Whether `ScopePrivate` can be instantiated for this client.
    """

    @staticmethod
    def local_dev() -> AuthClientConfig:
        return AuthClientConfig(
            release=Release.decode("local-dev"),
            secret="",
            supports_keycloak="allow",
            supports_internal="always",
            supports_personal="keycloak",
            supports_private="never",
        )

    @staticmethod
    def teams_client() -> AuthClientConfig:
        return AuthClientConfig(
            release=Release.teams_client(),
            secret="NANDAM_CLIENT_SECRET_TEAMS",  # noqa: S106
            supports_keycloak="trusted",
            supports_internal="never",
            supports_msgroup="keycloak",
            supports_personal="keycloak",
            supports_private="keycloak",
        )

    @staticmethod
    def unprotected(release: Release) -> AuthClientConfig:
        """
        Clients can "plug into" Nandam without being pre-authorized, but their
        scope will be limited to "internal" or "private", with the latter only
        available when the user is authenticated, such that someone is able to
        audit the agent's behaviour.
        """
        return AuthClientConfig(
            release=release,
            secret="",
            supports_keycloak="allow",
            supports_internal="always",
            supports_personal="never",
            supports_private="keycloak",
        )


class NdAuthConfig(BaseModel, frozen=True):
    clients: list[AuthClientConfig]

    @staticmethod
    def default() -> NdAuthConfig:
        return NdAuthConfig(
            clients=[
                AuthClientConfig.local_dev(),
                AuthClientConfig.teams_client(),
            ],
        )


@cache
def _read_auth_config() -> NdAuthConfig:
    try:
        config_yaml = BaseConfig.cfg_path("auth.yml").read_text()
        config = parse_yaml_as(NdAuthConfig, config_yaml)
        clients = bisect_make(
            [*NdAuthConfig.default().clients, *config.clients],
            key=lambda c: c.release,
        )
        return config.model_copy(update={"clients": clients})
    except Exception:
        logger.error("Failed to read config: auth.yml")  # noqa: TRY400
        return NdAuthConfig.default()


##
## Client
##


class ClientAuth(BaseModel, frozen=True):
    config: AuthClientConfig

    _x_authorization_client: str | None = PrivateAttr(default=None)

    @staticmethod
    def from_header(authorization: str | None) -> ClientAuth:
        # Confirm that the password matches the release's secret.
        release, secret = _parse_authorization_client(authorization)
        client_config = next(
            (
                client
                for client in _read_auth_config().clients
                if client.release == release
            ),
            None,
        )

        if not client_config:
            auth = ClientAuth(config=AuthClientConfig.unprotected(release))
        elif not client_config.secret:
            auth = ClientAuth(config=client_config)
        elif secret != BaseConfig.get(client_config.secret):
            raise AuthorizationError.unauthorized(f"invalid secret for '{release}'")
        else:
            auth = ClientAuth(config=client_config)

        return auth.model_copy(update={"_x_authorization_client": authorization})

    def as_header(self) -> str | None:
        return self._x_authorization_client


def _parse_authorization_client(authorization: str | None) -> tuple[Release, str]:
    if not authorization:
        return Release.decode("local-dev"), ""

    if not (credentials := parse_basic_credentials(authorization)):
        raise AuthorizationError.unauthorized(
            "bad X-Authorization-Client, expected 'Basic Base64(<release>:<secret>)'"
        )

    release_str, secret = credentials
    if release_obj := Release.try_decode(release_str):
        release = release_obj
    else:
        raise AuthorizationError.unauthorized(
            f"bad Authorization: invalid release: {release_str}"
        )

    return release, secret


##
## User
##


class UserAuth(BaseModel, frozen=True):
    user_id: str
    user_email: str
    user_name: str
    roles: list[str]
    groups: list[str]
    exp: int

    _x_authorization_user: str | None = PrivateAttr(default=None)

    @staticmethod
    def unsafe_create(
        *,
        user_id: str,
        user_email: str,
        user_name: str,
        roles: list[str] | None = None,
        groups: list[str] | None = None,
        exp: InternalExpiration | int = "request",
    ) -> UserAuth:
        return UserAuth(
            user_id=user_id,
            user_email=user_email,
            user_name=user_name,
            roles=roles or [],
            groups=groups or [],
            exp=exp if isinstance(exp, int) else UserAuth.create_exp(exp),
        )

    @staticmethod
    def create_exp(exp: InternalExpiration) -> int:
        exp_secs: int
        match exp:
            case "month":  # 30 days
                exp_secs = 30 * 24 * 60 * 60
            case "week":  # 7 days
                exp_secs = 7 * 24 * 60 * 60
            case "day":  # 1 day
                exp_secs = 24 * 60 * 60
            case "hour":  # 1 hour
                exp_secs = 60 * 60
            case "request":  # 10 minutes
                exp_secs = 10 * 60

        return int(time.time()) + exp_secs

    @staticmethod
    def from_header(authorization: str | None) -> UserAuth | None:
        if not authorization:
            if (
                not BaseConfig.is_kubernetes()
                and BaseConfig.debug.auth_user_email
                and BaseConfig.debug.auth_user_id
                and BaseConfig.debug.auth_user_name
            ):
                return UserAuth.unsafe_create(
                    user_id=BaseConfig.debug.auth_user_id,
                    user_email=BaseConfig.debug.auth_user_email,
                    user_name=BaseConfig.debug.auth_user_name,
                )

            return None

        if not authorization.startswith("Bearer "):
            raise AuthorizationError.unauthorized("expected Bearer token")

        jwt_token = authorization.removeprefix("Bearer ")
        try:
            header_base64 = jwt_token.split(".", 1)[0]
            header = json.loads(base64.urlsafe_b64decode(header_base64))
        except Exception:
            raise AuthorizationError.unauthorized("invalid JWT")  # noqa: B904

        if header.get("alg") == "HS256" or "." not in jwt_token:
            auth = _internal_jwt_decode(UserAuth, jwt_token)
        elif header.get("alg") == "RS256":
            auth = UserAuth._from_jwt_keycloak(jwt_token)
        else:
            raise AuthorizationError.unauthorized("invalid JWT")

        return auth.model_copy(update={"_x_authorization_user": authorization})

    @staticmethod
    def _from_jwt_keycloak(authorization: str) -> UserAuth:
        if not BaseConfig.auth.keycloak_audience:
            raise AuthorizationError.unauthorized("Keycloak not configured")
        try:
            jwt_token = authorization.removeprefix("Bearer ")
            signing_key = _get_jwks_client().get_signing_key_from_jwt(jwt_token)
            payload = jwt.decode(
                jwt_token,
                signing_key.key,
                algorithms=["RS256"],
                audience=BaseConfig.auth.keycloak_audience,
            )

            user_id = payload.get("oid")
            user_email = payload.get("email")
            user_name = payload.get("name")
            if not user_id or not user_email or not user_name:
                raise AuthorizationError.unauthorized("incomplete JWT")

            return UserAuth(
                user_id=user_id,
                user_email=user_email,
                user_name=user_name,
                roles=payload.get("roles") or [],
                groups=payload.get("groups") or [],
                exp=payload["exp"],
            )
        except AuthorizationError:
            raise
        except jwt.ExpiredSignatureError:
            raise AuthorizationError.unauthorized("expired JWT")  # noqa: B904
        except jwt.PyJWTError:
            raise AuthorizationError.unauthorized("invalid JWT")  # noqa: B904

    def as_header(self) -> str | None:
        if self._x_authorization_user:
            return self._x_authorization_user
        else:
            return self.as_internal()

    def as_internal(self) -> str | None:
        jwt_token = _internal_jwt_encode(self.model_dump())
        return f"Bearer {jwt_token}" if jwt_token else None


@cache
def _get_jwks_client() -> jwt.PyJWKClient:
    """
    Cache the JWKS client to reuse the connection pool and cached keys.
    """
    return jwt.PyJWKClient(
        f"https://login.microsoftonline.com/{BaseConfig.auth.keycloak_tenant_id}/discovery/v2.0/keys",
        lifespan=600,
    )


##
## Scope
##


def _validate_scope(  # noqa: C901, PLR0912
    auth_client: ClientAuth,
    auth_user_id: UserId | None,
    x_request_scope: str | None,
) -> Scope:
    if not x_request_scope:
        has_user = auth_user_id and auth_user_id.is_teams()
        if auth_client.config.supports_personal == "keycloak" and has_user:
            assert auth_user_id
            return ScopePersonal(user_id=auth_user_id)
        elif auth_client.config.supports_private != "never" and has_user:
            x_request_scope = "private"  # NOTE: No suffix => required user ID.
        elif auth_client.config.supports_internal == "always":
            return ScopeInternal()
        else:
            raise AuthorizationError.unauthorized("cannot infer scope")

    elif x_request_scope == "internal":
        if auth_client.config.supports_internal != "always":
            raise AuthorizationError.unauthorized(
                f"scope internal not supported by {auth_client.config.release}"
            )
        return ScopeInternal()

    elif x_request_scope.startswith("msgroup-"):
        if auth_client.config.supports_msgroup != "keycloak":
            raise AuthorizationError.unauthorized(
                f"scope msgroup not supported by {auth_client.config.release}"
            )
        if not auth_user_id:
            raise AuthorizationError.unauthorized("scope msgroup requires user auth")
        if not (scope := ScopeMsGroup.try_decode(x_request_scope)):
            raise AuthorizationError.unauthorized("scope msgroup is invalid")
        return scope

    elif x_request_scope == "personal":
        if auth_client.config.supports_personal == "never":
            raise AuthorizationError.unauthorized(
                f"scope personal not supported by {auth_client.config.release}"
            )
        elif not auth_user_id:
            raise AuthorizationError.unauthorized("scope personal requires user auth")
        else:
            return ScopePersonal(user_id=auth_user_id)

    # NOTE: Reaching this point results in a private scope.
    if auth_client.config.supports_private == "never":
        raise AuthorizationError.unauthorized("scope private not supported")
    elif auth_client.config.supports_private == "keycloak" and not auth_user_id:
        raise AuthorizationError.unauthorized("scope private requires user auth")

    if x_request_scope == "private":
        private_key = ""
    elif x_request_scope.startswith("private/"):
        private_key = x_request_scope.removeprefix("private/")
    else:
        raise AuthorizationError.unauthorized("invalid X-Request-Scope")

    if private_key:
        group_key = f"appgroup/{private_key}"
    elif auth_user_id and auth_user_id.is_teams():
        group_key = f"personal/{auth_user_id}"
    else:
        raise AuthorizationError.unauthorized(
            "scope private requires one of: private key suffix, user auth"
        )

    return ScopePrivate.generate(auth_client.config.release, group_key)


##
## Combined
##


class NdAuth(BaseModel, frozen=True):
    client: ClientAuth
    user: UserAuth | None
    scope: Scope
    request_id: RequestId
    x_user_id: str | None

    @staticmethod
    def stub(
        request_suffix: str = "",
        user_handle: str = "",
    ) -> NdAuth:
        parsed_handle = UserHandle.stub(user_handle)
        return NdAuth(
            client=ClientAuth(config=AuthClientConfig.unprotected(Release.stub())),
            user=(
                UserAuth.unsafe_create(
                    user_id=UserId.stub(parsed_handle).uuid(),
                    user_email=f"{parsed_handle}@mycompany.com",
                    user_name=str(parsed_handle),
                )
            ),
            scope=ScopeInternal(),
            request_id=RequestId.stub(request_suffix),
            x_user_id=None,
        )

    @staticmethod
    def from_headers(
        *,
        authorization: str | None = None,
        x_authorization_client: str | None = None,
        x_authorization_user: str | None = None,
        x_request_id: str | None = None,
        x_request_scope: str | None = None,
        x_user_id: str | None = None,
    ) -> NdAuth:
        if (
            authorization
            and not x_authorization_client
            and authorization.startswith("Basic ")
        ):
            x_authorization_client = authorization
        if (
            authorization
            and not x_authorization_user
            and authorization.startswith("Bearer ")
        ):
            x_authorization_user = authorization

        auth_client = ClientAuth.from_header(x_authorization_client)
        auth_user = (
            UserAuth.from_header(x_authorization_user)
            if auth_client.config.supports_keycloak != "ignore"
            else None
        )

        if auth_client.config.supports_keycloak == "required" and not auth_user:
            raise AuthorizationError.unauthorized(
                f"app {auth_client.config.release} requires Keycloak token"
            )

        auth_user_id: UserId | None = None
        if auth_user:
            auth_user_id = UserId.try_decode(f"user-{auth_user.user_id}")
            if not auth_user_id:
                raise AuthorizationError.unauthorized("invalid JWT: invalid user ID")
        elif auth_client.config.supports_keycloak == "trusted" and x_user_id:
            auth_user_id = UserId.try_decode(f"user-{x_user_id}")

        request_id = RequestId.try_decode(x_request_id)
        if x_request_id and not request_id:
            raise AuthorizationError.unauthorized(f"invalid request ID: {x_request_id}")
        if not request_id:
            request_id = RequestId.new()

        auth = NdAuth(
            client=auth_client,
            scope=_validate_scope(auth_client, auth_user_id, x_request_scope),
            user=auth_user,
            request_id=request_id,
            x_user_id=x_user_id,
        )
        auth.tracking_user_id()  # Assert that it can be inferred.
        return auth

    def as_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if value := self.client._x_authorization_client:  # noqa: SLF001
            headers["x-authorization-client"] = value
        if self.user and (value := self.user._x_authorization_user):  # noqa: SLF001
            headers["x-authorization-user"] = value
        if self.request_id:
            headers["x-request-id"] = str(self.request_id)
        if self.x_user_id:
            headers["x-user-id"] = self.x_user_id
        return headers

    def tracking_user_id(self) -> str:
        if self.user:
            return self.user.user_id
        elif self.x_user_id:
            return self.x_user_id
        elif self.client.config.release != "local-dev":
            return str(self.client.config.release)
        elif not BaseConfig.is_kubernetes() and BaseConfig.debug.auth_user_id:
            return BaseConfig.debug.auth_user_id
        else:
            raise AuthorizationError.unauthorized("cannot infer tracking user ID")

    def validated_user_id(self) -> UserId | None:
        """
        TODO: Should X-Authorization-User in an 'internal' scope be trusted?
        Current behaviour is to distrust it unless supports_keycloak=='trusted'.
        """
        if self.user:
            return UserId.try_decode(f"user-{self.user.user_id}")
        elif self.x_user_id and self.client.config.supports_keycloak == "trusted":
            return UserId.try_decode(f"user-{self.x_user_id}")
        else:
            return None


##
## Internal JWT
##


InternalExpiration = Literal["month", "week", "day", "hour", "request"]


def _internal_jwt_encode(payload: dict[str, Any]) -> str | None:
    if jwt_key := BaseConfig.auth.internal_secret:
        return jwt.encode(payload, jwt_key, algorithm="HS256")
    elif not BaseConfig.is_kubernetes():
        return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    else:
        return None


def _internal_jwt_decode[T: BaseModel](payload_type: type[T], jwt_token: str) -> T:
    try:
        if jwt_key := BaseConfig.auth.internal_secret:
            payload = jwt.decode(jwt_token, jwt_key, algorithms=["HS256"])
        elif not BaseConfig.is_kubernetes():
            payload = json.loads(base64.urlsafe_b64decode(jwt_token))
            if "exp" in payload and (
                not (exp := payload["exp"])
                or not isinstance(exp, int)
                or exp <= time.time()
            ):
                raise AuthorizationError.unauthorized("expired JWT")
        else:
            raise AuthorizationError.unauthorized("invalid JWT")

        return payload_type.model_validate(payload)
    except AuthorizationError:
        raise
    except jwt.ExpiredSignatureError:
        raise AuthorizationError.unauthorized("expired JWT")  # noqa: B904
    except jwt.PyJWTError:
        raise AuthorizationError.unauthorized("invalid JWT")  # noqa: B904
