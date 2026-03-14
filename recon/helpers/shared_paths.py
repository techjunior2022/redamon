"""Shared temp/output path helpers for Docker-in-Docker recon workflows."""

import os
import uuid
from pathlib import Path


CONTAINER_OUTPUT_ROOT = Path("/app/recon/output")
TEMP_SUBDIR_NAME = ".redamon-tmp"


def get_container_output_root() -> Path:
    return Path(os.environ.get("CONTAINER_RECON_OUTPUT_PATH", str(CONTAINER_OUTPUT_ROOT)))


def get_host_output_root() -> Path:
    host_output_path = os.environ.get("HOST_RECON_OUTPUT_PATH", "")
    if host_output_path:
        return Path(host_output_path)
    return get_container_output_root()


def get_container_shared_tmp_root() -> Path:
    return get_container_output_root() / TEMP_SUBDIR_NAME


def create_shared_temp_dir(prefix: str) -> Path:
    temp_dir = get_container_shared_tmp_root() / f".{prefix}_{uuid.uuid4().hex[:8]}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def to_host_path(container_path: str | Path) -> str:
    path_str = str(container_path)
    container_output_root = str(get_container_output_root())
    host_output_root = str(get_host_output_root())

    if host_output_root and path_str.startswith(container_output_root):
        return path_str.replace(container_output_root, host_output_root, 1)

    return path_str