"""Shared construction for the boto3 S3 clients that whylogs builds itself.

Works with Amazon S3 and any S3-compatible object store (for example
Backblaze B2, Cloudflare R2, or MinIO) by passing an ``endpoint_url`` and
matching credentials.
"""

from typing import Any, Optional

import boto3
from botocore.client import BaseClient
from botocore.config import Config


def _whylogs_version() -> str:
    try:
        from importlib import metadata
    except ImportError:  # Python < 3.8
        import importlib_metadata as metadata  # type: ignore

    try:
        return metadata.version("whylogs")
    except metadata.PackageNotFoundError:  # type: ignore
        return "dev"


def build_s3_client(config: Optional[Config] = None, **kwargs: Any) -> BaseClient:
    """Build an S3 client for clients whylogs creates itself.

    The whylogs identifier is appended to ``user_agent_extra`` so it is added to,
    not substituted for, any value already present on a caller-supplied ``config``.
    A custom ``endpoint_url`` for an S3-compatible store may be passed through
    ``kwargs``.
    """
    suffix = f"whylogs/{_whylogs_version()}"
    existing = getattr(config, "user_agent_extra", None)
    user_agent_extra = f"{existing} {suffix}" if existing else suffix
    config = (config or Config()).merge(Config(user_agent_extra=user_agent_extra))
    return boto3.client("s3", config=config, **kwargs)
