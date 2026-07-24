import ipaddress
import socket
from pathlib import Path
from urllib.parse import urlparse

import requests

import config


def _validate_public_https_url(url: str) -> None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().rstrip(".")

    if parsed.scheme != "https" or not host or parsed.username or parsed.password:
        raise ValueError("Invalid audio URL")
    if host not in config.ALLOWED_AUDIO_HOSTS:
        raise ValueError("Audio host is not allowed")

    addresses = {
        item[4][0]
        for item in socket.getaddrinfo(host, parsed.port or 443, type=socket.SOCK_STREAM)
    }
    if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise ValueError("Audio host resolved to a blocked address")


def download_http_audio(url: str, destination: str) -> None:
    _validate_public_https_url(url)

    with requests.get(
        url,
        stream=True,
        allow_redirects=False,
        timeout=(5, config.DOWNLOAD_TIMEOUT_SECONDS),
    ) as response:
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
        if content_type and not (
            content_type.startswith("audio/")
            or content_type == "application/octet-stream"
        ):
            raise ValueError("URL does not contain audio")

        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > config.MAX_AUDIO_BYTES:
            raise ValueError("Audio file is too large")

        downloaded = 0
        with Path(destination).open("wb") as output:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                downloaded += len(chunk)
                if downloaded > config.MAX_AUDIO_BYTES:
                    raise ValueError("Audio file is too large")
                output.write(chunk)
