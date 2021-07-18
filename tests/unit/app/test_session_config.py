from whylogs import SessionConfig
from uuid import uuid4


def test_config():
    name= uuid4()
    pipeline=uuid4()
    session_config = SessionConfig(name, pipeline,writers=[])
    
    assert session_config.project ==name
    assert session_config.pipeline == pipeline

    assert session_config.writers == []
    # session = session_from_config(session_config)


    