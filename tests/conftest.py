import pytest

from base.config import BaseConfig

import base.server.auth
import knowledge.domain.chunking


@pytest.fixture(autouse=True)
def setup_function():
    BaseConfig.environment = "local"

    # Reset defaults that may be overridden by some tests.
    base.server.auth.unittest_configure()
    knowledge.domain.chunking.unittest_configure()
