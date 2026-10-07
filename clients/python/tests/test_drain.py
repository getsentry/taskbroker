import os
import subprocess
import sys
from pathlib import Path
from unittest import mock

from taskbroker_client.drain import drain, main


def test_drain_creates_file_before_sleeping(tmp_path: Path) -> None:
    path = tmp_path / "draining"

    def sleep(seconds: float) -> None:
        assert path.exists()

    with mock.patch("taskbroker_client.drain.time.sleep", side_effect=sleep) as sleep_mock:
        drain(15, str(path))
    sleep_mock.assert_called_once_with(15)


def test_drain_touches_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "draining"
    path.touch()
    os.utime(path, (0, 0))
    with mock.patch("taskbroker_client.drain.time.sleep"):
        drain(0, str(path))
    assert path.stat().st_mtime > 0


def test_main_parses_seconds() -> None:
    with mock.patch("taskbroker_client.drain.drain") as drain_mock:
        main(["15"])
    drain_mock.assert_called_once_with(15.0)


def test_import_stays_light() -> None:
    code = (
        "import sys, taskbroker_client.drain; "
        "print(sorted(m for m in ('grpc', 'taskbroker_client.worker') if m in sys.modules))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "[]"
