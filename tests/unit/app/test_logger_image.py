import shutil

from PIL import Image

from whylogs.app.config import SessionConfig, WriterConfig
from whylogs.app.session import session_from_config
from whylogs.core.image_profiling import _METADATA_DEFAULT_ATTRIBUTES

_EXPECTED_COLUMNS = _METADATA_DEFAULT_ATTRIBUTES


def test_log_image(tmpdir, image_files):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
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
        for column_name in _EXPECTED_COLUMNS:
            assert column_name in columns, f"{column_name} not found in {columns}"
    shutil.rmtree(output_path, ignore_errors=True)


def test_log_pil_image(tmpdir, image_files):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])

    session = session_from_config(session_config)

    with session.logger("image_pil_test", with_rotation_time="1m", cache_size=1) as logger:

        for image_file_path in image_files:
            img = Image.open(image_file_path)
            logger.log_image(img)

        profile = logger.profile
        columns = profile.columns
        for column_name in _EXPECTED_COLUMNS:
            assert column_name in columns, f"{column_name} not found in {columns}"
    shutil.rmtree(output_path, ignore_errors=True)
