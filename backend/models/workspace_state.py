"""
Workspace State
===============

The models used by the workspace to maintain its state, including the registered
services and processes.
"""

from datetime import UTC, datetime
from pydantic import BaseModel, Field, SerializeAsAny
from pydantic.json_schema import JsonSchemaValue

from base.strings.process import ProcessName, ProcessUri
from base.strings.remote import RemoteProcessSecret, RemoteServiceSecret
from base.strings.scope import Workspace

from backend.server.context import ServiceConfig


class RegisteredService(BaseModel, frozen=True):
    workspace: Workspace
    config: SerializeAsAny[ServiceConfig]
    secret_key: RemoteServiceSecret
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class RegisteredProcess(BaseModel, frozen=True):
    process_uri: ProcessUri
    secret_key: RemoteProcessSecret
    name: ProcessName
    created_at: datetime
    arguments_schema: JsonSchemaValue
    progress_schema: JsonSchemaValue | None
    return_schema: JsonSchemaValue | None
