
import shutil
from whylogs.app.config import load_config
from whylogs.app.session import session_from_config,get_or_create_session
from whylogs.app.config import SessionConfig, WriterConfig

def test_segments(df_lending_club,tmpdir):
    output_path= tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)
    with session.logger("test", segments=[[{"key":"home_ownership","value":"RENT"}],[
                                          {"key":"home_ownership","value":"MORTGAGE"}]]
                                ,cache=1) as logger:
        logger.log_dataframe(df_lending_club)



def test_segments_keys(df_lending_club,tmpdir):
    output_path= tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)
    with session.logger("test", segments=["sub_grade","emp_title","home_ownership"],
                                cache=1) as logger:
        logger.log_dataframe(df_lending_club)


# def test_update_segments(profile_lending_club,tmpdir):
