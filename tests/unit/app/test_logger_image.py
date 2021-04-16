import shutil
from whylogs.app.session import session_from_config
from whylogs.app.config import SessionConfig, WriterConfig
from PIL import Image


def test_log_image(tmpdir, image_files):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])

    session = session_from_config(session_config)

    with session.logger("image_test") as logger:

        for image_file_path in image_files:
            logger.log_image(image_file_path)

        profile = logger.profile
        columns = profile.columns
        assert len(columns) == 19
    shutil.rmtree(output_path)


def test_log_pil_image(tmpdir, image_files):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])

    session = session_from_config(session_config)

    with session.logger(
        "image_pil_test", with_rotation_time="s", cache_size=1
    ) as logger:

        for image_file_path in image_files:
            img = Image.open(image_file_path)
            logger.log_image(img)

        profile = logger.profile
        columns = profile.columns
        assert len(columns) == 19
    shutil.rmtree(output_path)
