from base.server.auth import (
    _internal_jwt_decode,
    _internal_jwt_encode,
    InternalExpiration,
    unittest_configure,
    UserAuth,
)


def _given_internal_user_auth(
    *,
    user_id: str,
    user_email: str,
    user_name: str,
    roles: list[str] | None = None,
    groups: list[str] | None = None,
    exp: InternalExpiration = "request",
    auth_internal_secret: bool,
) -> UserAuth:
    unittest_configure(auth_internal_secret=auth_internal_secret)

    stub_auth = UserAuth.unsafe_create(
        user_id=user_id,
        user_email=user_email,
        user_name=user_name,
        roles=roles,
        groups=groups,
        exp=exp,
    )
    jwt_token = _internal_jwt_encode(stub_auth.model_dump())
    assert jwt_token is not None

    parsed_auth = _internal_jwt_decode(UserAuth, jwt_token)
    assert parsed_auth == stub_auth

    assert ("." in jwt_token) == auth_internal_secret

    return parsed_auth


def test_internal_jwt_with_secret() -> None:
    auth = _given_internal_user_auth(
        user_id="user-123",
        user_email="test@example.com",
        user_name="Test User",
        auth_internal_secret=True,
    )
    assert auth is not None
    assert auth.user_id == "user-123"
    assert auth.user_email == "test@example.com"
    assert auth.user_name == "Test User"


def test_internal_jwt_without_secret() -> None:
    auth = _given_internal_user_auth(
        user_id="user-123",
        user_email="test@example.com",
        user_name="Test User",
        auth_internal_secret=False,
    )
    assert auth is not None
    assert auth.user_id == "user-123"
    assert auth.user_email == "test@example.com"
    assert auth.user_name == "Test User"
