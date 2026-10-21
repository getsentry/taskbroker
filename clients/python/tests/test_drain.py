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


def test_main_parses_seconds() -> None:
    with mock.patch("taskbroker_client.drain.drain") as drain_mock:
        main(["15"])
    drain_mock.assert_called_once_with(15.0)
