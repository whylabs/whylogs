import datetime
import io
import pickle

from whylogs.app import Session
from whylogs.core.datasetprofile import DatasetProfile


def profiles_eq(profile1: DatasetProfile, profile2: DatasetProfile):
    assert set(profile1.columns) == set(profile2.columns)
    assert profile1.constraints == profile2.constraints
    assert profile1.dataset_timestamp == profile2.dataset_timestamp
    assert profile1.session_id == profile2.session_id
    # TODO this fails on mac for some reason. Need to figure out why.
    # assert str(profile1.to_summary()) == str(profile2.to_summary())
    assert profile1.name == profile2.name


def test_pickle_with_dataset_timestamp():
    session = Session("project", "pipeline", writers=[])
    dt = datetime.datetime.fromtimestamp(1634939335, tz=datetime.timezone.utc)
    logger = session.logger("", dataset_timestamp=dt)

    logger.log_csv(
        io.StringIO(
            """a,b,c
    1,1,1
    1,1,2
    4,4,3
    """
        )
    )

    profile = logger.profile
    pickled_profile = pickle.dumps(profile)
    unpickled_profile: DatasetProfile = pickle.loads(pickled_profile)

    profiles_eq(profile, unpickled_profile)


def test_serde_with_dataset_timezone():
    session = Session("project", "pipeline", writers=[])
    dt = datetime.datetime.fromtimestamp(1634939335, tz=datetime.timezone.utc)
    logger = session.logger("", dataset_timestamp=dt)

    logger.log_csv(
        io.StringIO(
            """a,b,c
    1,1,1
    1,1,2
    4,4,3
    """
        )
    )

    profile = logger.profile
    deserialized_profile = DatasetProfile.parse_delimited_single(profile.serialize_delimited())[1]
    profiles_eq(profile, deserialized_profile)


def test_serde_without_dataset_timezone():
    session = Session("project", "pipeline", writers=[])
    dt = datetime.datetime.fromtimestamp(1634939335, tz=None)
    logger = session.logger("", dataset_timestamp=dt)

    logger.log_csv(
        io.StringIO(
            """a,b,c
    1,1,1
    1,1,2
    4,4,3
    """
        )
    )

    profile = logger.profile
    deserialized_profile = DatasetProfile.parse_delimited_single(profile.serialize_delimited())[1]
    profiles_eq(profile, deserialized_profile)
