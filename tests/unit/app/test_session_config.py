from uuid import uuid4

from whylogs import SessionConfig


def test_config():
    name = uuid4()
    pipeline = uuid4()
    session_config = SessionConfig(name, pipeline, writers=[])

    assert session_config.project == name
    assert session_config.pipeline == pipeline

    assert session_config.writers == []
