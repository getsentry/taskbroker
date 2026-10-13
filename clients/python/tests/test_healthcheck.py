from pathlib import Path

import pytest

from taskbroker_client.healthcheck import main


def test_fails_when_worker_has_not_touched_file_since_last_check(tmp_path: Path) -> None:
    path = tmp_path / "health"
    path.touch()

    main([str(path)])
    assert not path.exists()

    with pytest.raises(SystemExit) as exc:
        main([str(path)])
    assert exc.value.code == (
        f"{path} is missing: the worker has not touched it since the last probe"
    )
