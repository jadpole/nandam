import pytest

import knowledge.domain.chunking


@pytest.fixture(autouse=True)
def setup_function():
    # Reset defaults that may be overridden by some tests.
    knowledge.domain.chunking.unittest_configure()
