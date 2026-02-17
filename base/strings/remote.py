from datetime import datetime

from base.core.strings import ValidatedStr
from base.core.unique_id import unique_id_from_datetime


##
## Worker
##


NUM_CHARS_REMOTE_WORKER_SECRET = 40
REGEX_REMOTE_WORKER_SECRET = r"nrs-[a-z0-9]{40}"  # noqa: S105


class RemoteServiceSecret(ValidatedStr):
    @staticmethod
    def generate(timestamp: datetime | None = None) -> RemoteServiceSecret:
        unique_id = unique_id_from_datetime(timestamp, NUM_CHARS_REMOTE_WORKER_SECRET)
        return RemoteServiceSecret.decode(f"nrs-{unique_id}")

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_REMOTE_WORKER_SECRET


##
## Process
##


NUM_CHARS_REMOTE_PROCESS_SECRET = 40
REGEX_REMOTE_PROCESS_SECRET = r"nrp-[a-z0-9]{40}"  # noqa: S105


class RemoteProcessSecret(ValidatedStr):
    """
    A unique ID sent to an external service that executes a process.

    This ID is mapped to the underlying process URI by the system and is used as
    a proof of write-access on the process status.  Hence, it should be treated
    like a secret (though a short-lived one).
    """

    @staticmethod
    def generate(timestamp: datetime | None = None) -> RemoteProcessSecret:
        unique_id = unique_id_from_datetime(timestamp, NUM_CHARS_REMOTE_PROCESS_SECRET)
        return RemoteProcessSecret.decode(f"nrp-{unique_id}")

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_REMOTE_PROCESS_SECRET
