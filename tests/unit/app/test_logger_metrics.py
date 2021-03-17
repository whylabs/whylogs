import shutil
from whylogs.app.session import session_from_config
from whylogs.app.config import SessionConfig, WriterConfig


def test_log_metrics(tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig(
        "project", "pipeline", writers=[writer_config])

    session = session_from_config(session_config)
    with session.logger("metrics_test") as logger:

        targets = ["class_name1", "class_name2", "class_name3"]

        predictions = ["class_name1", "class_name2", "class_name2"]
        scores = [0.2, 0.5, 0.6]
        logger.log_metrics(targets, predictions, scores)

        profile = logger.profile
        metrics_profile = profile.model_profile

        assert metrics_profile is not None
        num_labels = 3
        assert len(metrics_profile.metrics.confusion_matrix.labels) == num_labels
    shutil.rmtree(output_path)
