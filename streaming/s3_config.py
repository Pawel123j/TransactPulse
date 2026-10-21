"""Shared S3 transport settings for streaming and batch Spark jobs."""

from urllib.parse import urlsplit


def ssl_enabled(endpoint: str) -> str:
    """Preserve HTTPS; reject malformed endpoints before passing credentials."""
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ValueError("S3_ENDPOINT must be an http(s) origin without credentials or path")
    # Accessing .port also rejects invalid or out-of-range port values.
    _ = parsed.port
    return "true" if parsed.scheme == "https" else "false"
