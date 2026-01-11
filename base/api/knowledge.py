from pydantic import BaseModel, Field
from typing import Literal


class KnowledgeSettings(BaseModel, frozen=True):
    creds: dict[str, str] = Field(
        default_factory=dict,
        examples=[{"gitlab": "gplat-xxxxxxxxxxxxxxxxxxxx"}],
    )
    """
    Credentials used by the Knowledge connectors, overriding the defaults.
    The key is the Realm of the connector and the value is the Authorization.
    """
    prefix_rules: list[tuple[str, Literal["allow", "block"]]] = Field(
        default_factory=list,
        examples=[
            ("ndk://microsoft/", "block"),
            ("ndk://microsoft/sharepoint-MsSiteId/", "allow"),
        ],
    )
    """
    In "block" mode, "ndk://" prefixes for which `resolve` should fail.

    Useful when called in a public or semi-public scope where we must omit the
    URIs that might leak confidential information from the results.
    """
