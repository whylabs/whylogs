"""
Class and functions for whylogs logging
"""
import datetime
from typing import List, Optional, Dict, Union

import pandas as pd
from pandas._typing import FilePathOrBuffer

from whylogs.app.writers import Writer
from whylogs.core import DatasetProfile

import hashlib
import json

import numpy as np
TIME_ROTATION_VALUES = ["s", "m", "h", "d"]


# TODO upgrade to Classes
SegmentTag = Dict[str, any]
SegmentTags = List[SegmentTag]


class Logger:
    """
    Class for logging whylogs statistics.

    :param session_id: The session ID value. Should be set by the Session boject
    :param dataset_name: The name of the dataset. Gets included in the DatasetProfile metadata and can be used in generated filenames.
    :param dataset_timestamp: Optional. The timestamp that the logger represents
    :param session_timestamp: Optional. The time the session was created
    :param tags: Optional. Dictionary of key, value for aggregating data upstream
    :param metadata: Optional. Dictionary of key, value. Useful for debugging (associated with every single dataset profile)
    :param writers: List of Writer objects used to write out the data
    :param with_rotation_time. Whether to rotate with time, takes values of overall rotation interval,
            "s" for seconds
            "m" for minutes
            "h" for hours
            "d" for days
    :param verbose: enable debug logging or not

    :param cache: set how many dataprofiles to cache
    :param segments: define either a list of egment keys or a list of segments tags: [  {"key":<featurename>,"value": <featurevalue>},... ]
    :param profile_full_data: when segmenting dataset, an option to keep the full unsegmented profile of the dataset.
    """

    def __init__(self,
                 session_id: str,
                 dataset_name: str,
                 dataset_timestamp: Optional[datetime.datetime] = None,
                 session_timestamp: Optional[datetime.datetime] = None,
                 tags: Dict[str, str] = {},
                 metadata: Dict[str, str] = None,
                 writers=List[Writer],
                 verbose: bool = False,
                 with_rotation_time: Optional[str] = None,
                 interval: int = 1,
                 cache: int = 1,
                 segments: Optional[Union[List[SegmentTags], List[str]]] = None,
                 profile_full_dataset: bool = False,
                 ):
        """
        """
        self._active = True

        if session_timestamp is None:
            self.session_timestamp = datetime.datetime.now(
                datetime.timezone.utc)
        else:
            self.session_timestamp = session_timestamp
        self.dataset_name = dataset_name
        self.writers = writers
        self.verbose = verbose
        self.cache = cache
        self.tags = tags
        self.session_id = session_id
        self.metadata = metadata
        self.profile_full_dataset = profile_full_dataset

        self.set_segments(segments)

        self._profiles = []
        self._intialize_profiles(dataset_timestamp)
        # intialize to seconds in the day
        self.interval = interval
        self.with_rotation_time = with_rotation_time
        self.set_rotation(with_rotation_time)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def profile(self,)->DatasetProfile:
        """
        :return: the last backing dataset profile
        :rtype: DatasetProfile
        """
        return self._profiles[-1]["full_profile"]

    @property
    def segmented_profiles(self,)->Dict[str, DatasetProfile]:
        """
        :return: the last backing dataset profile
        :rtype: Dict[str, DatasetProfile]
        """
        return self._profiles[-1]["segmented_profiles"]

    def get_segment(self, segment_tags: SegmentTags, copy: bool = False)->Optional[DatasetProfile]:

        hashed_seg = hash_segment(segment_tags)
        segment_profile = self._profiles[-1]["segmented_profiles"].get(
            hashed_seg, None)
        if copy and (segment_profile is not None):
            return segment_profile.copy()
        else:
            return segment_profile

    def set_segments(self, segments: Union[List[SegmentTags], List[str]]) -> None:
        if segments:
            if all(isinstance(elem, str) for elem in segments):
                self.segment_type = "keys"
                self.segments = segments
            else:
                self.segments = segments
                self.segment_type = "set"
        else:
            self.segments = None
            self.segment_type = None

    def _intialize_profiles(self,
                            dataset_timestamp: Optional[datetime.datetime] = datetime.datetime.now(datetime.timezone.utc)) -> None:

        full_profile = None
        if self.full_profile_check():
            full_profile = DatasetProfile(
                self.dataset_name,
                dataset_timestamp=dataset_timestamp,
                session_timestamp=self.session_timestamp,
                tags=self.tags,
                metadata=self.metadata,
                session_id=self.session_id
            )
        self._profiles.append(
            {"full_profile": full_profile, "segmented_profiles": {}})

    def set_rotation(self, with_rotation_time: str = None):

        if with_rotation_time is not None:
            self.with_rotation_time = with_rotation_time.lower()

            if self.with_rotation_time == 's':
                interval = 1  # one second
                self.suffix = "%Y-%m-%d_%H-%M-%S"
                self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}(\.\w+)?$"
            elif self.with_rotation_time == 'm':
                interval = 60  # one minute
                self.suffix = "%Y-%m-%d_%H-%M"
                self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}(\.\w+)?$"
            elif self.with_rotation_time == 'h':
                interval = 60 * 60  # one hour
                self.suffix = "%Y-%m-%d_%H"
                self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}(\.\w+)?$"
            elif self.with_rotation_time == 'd':
                interval = 60 * 60 * 24  # one day
                self.suffix = "%Y-%m-%d"
                self.extMatch = r"^\d{4}-\d{2}-\d{2}(\.\w+)?$"
            else:
                raise TypeError("Invalid choice of rotation time, valid choices are {}".format(
                    TIME_ROTATION_VALUES))
            # time in seconds
            current_time = int(datetime.datetime.utcnow().timestamp())
            self.interval = interval * self.interval
            self.rotate_at = self.rotate_when(current_time)

    def rotate_when(self, time):
        result = time + self.interval
        return result

    def should_rotate(self,):

        if self.with_rotation_time is None:
            return False

        current_time = int(datetime.datetime.utcnow().timestamp())
        if current_time >= self.rotate_at:
            return True
        return False

    def _rotate_time(self):
        """
        rotate with time add a suffix
        """
        current_time = int(datetime.datetime.utcnow().timestamp())
        # get the time that this current logging rotation started
        sequence_start = self.rotate_at - self.interval
        timeTuple = datetime.datetime.fromtimestamp(sequence_start)
        rotation_suffix = "." + timeTuple.strftime(self.suffix)
        log_datetime = datetime.datetime.strptime(
            timeTuple.strftime(self.suffix), self.suffix)

        # modify the segment datetime stamps
        if (self.segments is None) or ((self.segments is not None) and self.profile_full_dataset):
            self._profiles[-1]["full_profile"].dataset_timestamp = log_datetime
        if self.segments is not None:
            for _, each_prof in self._profiles[-1]["segmented_profiles"].items():
                each_prof.dataset_timestamp = log_datetime

        self.flush(rotation_suffix)

        if len(self._profiles) > self.cache:
            self._profiles[-self.cache-1] = None

        self._intialize_profiles()

        # compute new rotate_at and while loop in case current function
        # takes longer than interval
        self.rotate_at = self.rotate_when(current_time)
        while self.rotate_at <= current_time:
            self.rotate_at += self.interval

    def flush(self, rotation_suffix: str = None):
        """
        Synchronously perform all remaining write tasks
        """
        if not self._active:
            print("WARNING: attempting to flush a closed logger")
            return None

        for writer in self.writers:
            # write full profile

            if self.full_profile_check():

                if rotation_suffix is None:
                    writer.write(self._profiles[-1]["full_profile"])
                else:
                    writer.write(
                        self._profiles[-1]["full_profile"], rotation_suffix)

            if self.segments is not None:

                for hashseg, each_seg_prof in self._profiles[-1]["segmented_profiles"].items():
                    seg_suffix = hashseg
                    full_suffix = "_"+seg_suffix
                    if rotation_suffix is None:
                        writer.write(each_seg_prof, full_suffix)
                    else:
                        full_suffix += rotation_suffix
                        writer.write(each_seg_prof, full_suffix)

    def full_profile_check(self,)->bool:
        """
        returns a bool to determine if unsegmented dataset should be profiled.
        """
        return ((self.segments is None) or ((self.segments is not None) and self.profile_full_dataset))

    def close(self) -> Optional[DatasetProfile]:
        """
        Flush and close out the logger, outputs the last profile

        :return: the result dataset profile. None if the logger is closed
        """
        if not self._active:
            print("WARNING: attempting to close a closed logger")
            return None
        if self.with_rotation_time is None:
            self.flush()
        else:
            self._rotate_time()
            _ = self._profiles.pop()

        self._active = False
        profile = self._profiles[-1]["full_profile"]
        self._profiles = None
        return profile

    def log(
        self,
        features: Optional[Dict[str, any]] = None,
        feature_name: str = None,
        value: any = None,
        segments: Optional[Union[List[SegmentTags], List[str]]] = None,
        profile_full_dataset: bool = False,
    ):
        """
        Logs a collection of features or a single feature (must specify one or the other).

        :param features: a map of key value feature for model input
        :param feature_name: a dictionary of key->value for multiple features. Each entry represent a single columnar feature
        :param feature_name: name of a single feature. Cannot be specified if 'features' is specified
        :param value: value of as single feature. Cannot be specified if 'features' is specified
        :param segment: define either a list of egment keys or a list of segments tags:
        `[{"key":<featurename>,"value": <featurevalue>},... ]`
        :param profile_full_data: when segmenting dataset, an option to keep the full unsegmented profile of the dataset.

        """
        if not self._active:
            return None

        if self.should_rotate():
            self._rotate_time()

        # segmnet check  in case segments are just keys
        self.profile_full_dataset = profile_full_dataset
        if (segments is not None):
            self.set_segments(segments)

        if features is None and feature_name is None:
            return

        if features is not None and feature_name is not None:
            raise ValueError("Cannot specify both features and feature_name")

        if features is not None:
            # full profile
            self.log_dataframe(pd.DataFrame([features]))
        else:
            if self.full_profile_check():
                self._profiles[-1]["full_profile"].track_datum(
                    feature_name, value)

            if self.segments:
                self.log_segment_datum(feature_name, value)

    def log_segment_datum(self, feature_name, value):
        segment_tags = []
        segment_tags.append(
            {"key": feature_name, "value": value})
        segment_profile = self.get_segment(segment_tags)
        if (segment_type == "keys"):
            if (feature_name in self.segments):

                if segment_profile is None:
                    return
                else:
                    segment_profile.track_datum(feature_name, value)
            else:
                for each_profile in self._profiles[-1]["segmented_profiles"]:
                    each_profile.track_datum(feature_name, value)
        elif (segment_type == "set"):
            if segment_tags not in self.segments:
                return
            else:
                segment_profile.track_datum(feature_name, value)

    def log_csv(self,
                filepath_or_buffer: FilePathOrBuffer,
                segments: Optional[Union[List[SegmentTags], List[str]]] = None,
                profile_full_dataset: bool = False, **kwargs,):
        """
        Log a CSV file. This supports the same parameters as :func`pandas.red_csv<pandas.read_csv>` function.

        :param filepath_or_buffer: the path to the CSV or a CSV buffer
        :type filepath_or_buffer: FilePathOrBuffer
        :param kwargs: from pandas:read_csv
        :param segment: define either a list of egment keys or a list of segments tags: `[  {"key":<featurename>,"value": <featurevalue>},... ]`
        :param profile_full_data: when segmenting dataset, an option to keep the full unsegmented profile of the dataset.
        """
        if not self._active:
            return
        if self.should_rotate():
            self._rotate_time()

        self.profile_full_dataset = profile_full_dataset
        if (segments is not None):
            self.set_segments(segments)

        df = pd.read_csv(filepath_or_buffer, **kwargs)
        self.log_dataframe(df)

    def log_dataframe(self, df,
                      segments: Optional[Union[List[SegmentTags], List[str]]] = None,
                      profile_full_dataset: bool = False,):
        """
        Generate and log a whylogs DatasetProfile from a pandas dataframe
        :param df: the Pandas dataframe to log
        """
        if not self._active:
            return None
        if self.should_rotate():
            self._rotate_time()

        # segmnet check  in case segments are just keys
        self.profile_full_dataset = profile_full_dataset
        if (segments is not None):
            self.set_segments(segments)

        if self.full_profile_check():
            self._profiles[-1]["full_profile"].track_dataframe(df)

        if self.segments:
            self.log_segments(df)

    def log_segments(self, data):

        if self.segment_type == "keys":
            self.log_segments_keys(data)
        elif self.segment_type == "set":
            self.log_fixed_segments(data)
        else:
            raise TypeError("segments type not supported")

    def log_segments_keys(self, data):
        try:
            grouped_data = data.groupby(self.segments)
        except KeyError as e:
            raise e

        segments = grouped_data.groups.keys()

        for each_segment in segments:
            try:
                segment_df = grouped_data.get_group(each_segment)
                segment_tags = []
                for i in range(len(self.segments)):
                    segment_tags.append(
                        {"key": self.segments[i], "value": each_segment[i]})
                self.log_df_segment(segment_df, segment_tags)
            except KeyError as e:
                continue

    def log_fixed_segments(self, data):

        # we group each segment seperately since the segment tags are allowed
        # to overlap

        for segment_tag in self.segments:
            # create keys
            segment_keys = [feature["key"] for feature in segment_tag]
            seg = tuple([feature["value"] for feature in segment_tag])

            grouped_data = data.groupby(segment_keys)

            if len(seg) == 1:
                seg = seg[0]
            # check if segment exist
            if seg not in grouped_data.groups:
                continue
            segment_df = grouped_data.get_group(seg)

            self.log_df_segment(segment_df, segment_tag)

    def log_df_segment(self, df, segment_tags: SegmentTags):
        segment_tags = sorted(segment_tags, key=lambda x: x["key"])

        segment_profile = self.get_segment(segment_tags)
        if segment_profile is None:
            segment_profile = DatasetProfile(
                self.dataset_name,
                dataset_timestamp=datetime.datetime.now(datetime.timezone.utc),
                session_timestamp=self.session_timestamp,
                tags={**self.tags, **{"segment": json.dumps(segment_tags)}},
                metadata=self.metadata,
                session_id=self.session_id
            )
            segment_profile.track_dataframe(df)
            hashed_seg = hash_segment(segment_tags)
            self._profiles[-1]["segmented_profiles"][hashed_seg] = segment_profile
        else:
            segment_profile.track_dataframe(df)

    def is_active(self):
        """
        Return the boolean state of the logger
        """
        return self._active


def hash_segment(seg: List[Dict])-> str:
    return hashlib.sha256(json.dumps(seg).encode('utf-8')).hexdigest()
