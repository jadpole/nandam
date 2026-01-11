from prometheus_client import Counter, Histogram

from base.server.metrics import *  # noqa: F403
from base.strings.data import MimeType
from base.strings.resource import Observable, Realm

from knowledge.models.storage import Locator


##
## Metrics
##


METRIC_QUERY_REQUEST = Counter(
    "knowledge_query_request",
    "",
    labelnames=[],
)

METRIC_LOCATOR = Histogram(
    "knowledge_locator_duration_seconds",
    "",
    labelnames=["realm", "locator", "success"],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)

METRIC_QUERY_EXECUTION = Histogram(
    "knowledge_query_execution_duration_seconds",
    "",
    labelnames=["realm", "locator", "mime_type", "success"],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)

METRIC_RESOLVE = Histogram(
    "knowledge_resolve_duration_seconds",
    "",
    labelnames=["realm", "locator", "success"],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)

METRIC_READ = Histogram(
    "knowledge_read_duration_seconds",
    "",
    labelnames=[
        "realm",
        "locator",
        "observable",
        "mime_type",
        "success",
    ],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)

METRIC_INGESTION = Histogram(
    "knowledge_ingestion_duration_seconds",
    "",
    labelnames=["realm", "locator", "observable", "success"],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)

METRIC_GENERATE_DESCRIPTION = Histogram(
    "knowledge_generate_description_duration_seconds",
    "duration to generate one description for a resource or observable",
    labelnames=["realm", "locator", "success"],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)

METRIC_GENERATE_DESCRIPTIONS = Histogram(
    "knowledge_generate_descriptions_duration_seconds",
    "duration to generate all descriptions for a resource or observable",
    labelnames=["realm", "locator", "success"],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)


##
## Effects
##


def track_request() -> None:
    METRIC_QUERY_REQUEST.inc()


def track_locator_duration(
    realm: Realm | None,
    locator: Locator | None,
    duration_secs: float,
) -> None:
    METRIC_LOCATOR.labels(
        realm=str(realm) if realm else "unknown",
        locator=locator.kind if locator else "unknown",
        success=str(bool(locator)).lower(),
    ).observe(duration_secs)


def track_query_execution(
    locator: Locator,
    mime_type: MimeType | None,
    success: bool,
    duration_secs: float,
) -> None:
    METRIC_LOCATOR.labels(
        realm=str(locator.realm),
        locator=locator.kind,
        mime_type=str(mime_type) if mime_type else "unknown",
        success=str(success).lower(),
    ).observe(duration_secs)


def track_resolve_duration(
    locator: Locator,
    success: bool,
    duration_secs: float,
) -> None:
    METRIC_RESOLVE.labels(
        realm=str(locator.realm),
        locator=locator.kind,
        success=str(success).lower(),
    ).observe(duration_secs)


def track_read_duration(
    locator: Locator,
    observable: Observable,
    mime_type: MimeType | None,
    success: bool,
    duration_secs: float,
) -> None:
    METRIC_READ.labels(
        realm=str(locator.realm),
        locator=locator.kind,
        observable=observable.suffix_kind(),
        mime_type=str(mime_type) if mime_type else "unknown",
        success=str(success).lower(),
    ).observe(duration_secs)


def track_ingestion_duration(
    locator: Locator,
    observable: Observable,
    success: bool,
    duration_secs: float,
) -> None:
    METRIC_INGESTION.labels(
        realm=str(locator.realm),
        locator=locator.kind,
        observable=observable.suffix_kind(),
        success=str(success).lower(),
    ).observe(duration_secs)


# TODO: Use this metric.
def track_generate_description_duration(
    locator: Locator,
    mime_type: MimeType | None,
    success: bool,
    duration_secs: float,
) -> None:
    resource_uri = locator.resource_uri()
    METRIC_GENERATE_DESCRIPTION.labels(
        realm=str(resource_uri.realm),
        locator=locator.kind,
        mime_type=str(mime_type) if mime_type else "unknown",
        success=str(success).lower(),
    ).observe(duration_secs)


# TODO: Use this metric.
def track_generate_descriptions_duration(
    locator: Locator,
    mime_type: MimeType | None,
    success: bool,
    duration_secs: float,
) -> None:
    resource_uri = locator.resource_uri()
    METRIC_GENERATE_DESCRIPTIONS.labels(
        realm=str(resource_uri.realm),
        locator=locator.kind,
        mime_type=str(mime_type) if mime_type else "unknown",
        success=str(success).lower(),
    ).observe(duration_secs)
