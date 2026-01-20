import logging
import os

from base.config import BaseConfig
from base.strings.data import MimeType

logger = logging.getLogger(__name__)


IMAGE_MAX_SIDE_PX = 1024
"""
Image blobs should be downscaled to at most 1024x1024 before being fed to LLMs.
Larger images would automatically be downscaled by the LLM APIs anyway, so more
pixels only increase the latency.

For reference:

- GPT-4o supports at most 8 tiles: 1024x1024 or 2048x768 (1:2.6).
- Claude 3 supports at most: 1092x1092 or 1568x784 (1:2).

NOTE: In Knowledge, images are typically also available as `$file`, allowing
tools to work on full-resolution images and users to download them.
"""

IMAGE_MIN_SIDE_PX = 256
"""
Discard small images:
- LLMs struggle to understand images 200x200 and smaller;
- Small images are often meaningless (e.g., thumbnails).

NOTE: The choice of 256x256 is just a heuristic, equivalent to half of a GPT-4
vision tile (512x512).  There is no deeper reason.
"""

IMAGE_MIME_TYPES: list[MimeType] = [
    MimeType.decode("image/png"),
    MimeType.decode("image/jpeg"),
    MimeType.decode("image/webp"),
    MimeType.decode("image/heic"),
    MimeType.decode("image/heif"),
]
"""
These are the image MIME types that can be ingested by Knowledge, then converted
into `TARGET_IMAGE_TYPE`.  LLMs therefore rarely see the original image, which
improve performance and makes it easier to swap LLMs.

NOTE: Other types of image blobs are discarded when they are ingested into
Knowledge, i.e., instead of using an "embed" link with an `$media` URI, these
are represented as plain text `![label](#filename.ext)` in `ContentText`.
"""

IMAGE_PREFERRED_TYPE = MimeType("image/webp")
"""
All images are converted into WEBP format by Knowledge, which is more efficient
than PNG for sending its base64 data in HTTP requests, especially to LLMs, since
requests to our LLM Gateway must be under 30MB.
"""


class StorageConfig:
    aws_access_key = os.getenv("KNOWLEDGE_AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("KNOWLEDGE_AWS_SECRET_KEY")
    encryption_key = os.getenv("KNOWLEDGE_ENCRYPTION_KEY")
    s3_arn = os.getenv("KNOWLEDGE_AWS_S3_ARN")
    s3_name = os.getenv("KNOWLEDGE_AWS_S3_NAME")
    s3_region = os.getenv("KNOWLEDGE_AWS_S3_REGION")


class WebServerConfig:
    port = int(os.getenv("PORT", "8020"))


class KnowledgeConfig(BaseConfig):
    storage = StorageConfig()
    web_server = WebServerConfig()
