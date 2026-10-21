"""Create new private local Airflow settings; never replace existing credentials."""

import json
import os
import secrets
import sys
from pathlib import Path


def initialize(directory: Path) -> Path:
    directory = directory.resolve()
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    passwords = directory / "passwords.json"
    passwords.touch(mode=0o600)
    passwords.write_text(json.dumps({"admin": secrets.token_urlsafe(32)}))
    env = directory / "airflow.env"
    env.touch(mode=0o600)
    env.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        f"DOCKER_SOCKET_GID={Path('/var/run/docker.sock').stat().st_gid}\n"
        f"AIRFLOW_DB_PASSWORD={secrets.token_hex(32)}\n"
        f"AIRFLOW_JWT_SECRET={secrets.token_hex(48)}\n"
        f"AIRFLOW_AUTH_PASSWORDS_FILE='{passwords}'\n"
    )
    return env


if __name__ == "__main__":
    print(initialize(Path(sys.argv[1])))  # noqa: T201 -- path only, no secrets
