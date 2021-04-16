"""
Class and functions for whylogs logging
"""
import datetime
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import AnyStr, Callable, Dict, List, Optional, Union
from typing.io import IO

import pandas as pd
from tqdm import tqdm

from whylogs.app.writers import Writer
from whylogs.core import (
    METADATA_DEFAULT_ATTRIBUTES,
    DatasetProfile,
    TrackBB,
    TrackImage,
)
from whylogs.core.statistics.constraints import DatasetConstraints
from whylogs.io import LocalDataset
from whylogs.proto import ModelType

# TODO upgrade to Classes
SegmentTag = Dict[str, any]
Segment = List[SegmentTag]

logger = logging.getLogger(__name__)


class Logger:
    """
    Class for logging whylogs statistics.

    :param session_id: The session ID value. Should be set by the Session boject
    :param dataset_name: The name of the dataset. Gets included in the DatasetProfile metadata and can be used in generated filenames.
    :param dataset_timestamp: Optional. The timestamp that the logger represents
    :param session_timestamp: Optional. The time the session was created
    :param tags: Optional. Dictionary of key, value for aggregating data upstream
    :param metadata: Optional. Dictionary of key, value. Useful for debugging (associated with every single dataset profile)
    :param writers: Optional. List of Writer objects used to write out the data
    :param with_rotation_time: Optional. Log rotation interval, \
            consisting of digits with unit specification, e.g. 30s, 2h, d.\
            units are seconds ("s"), minutes ("m"), hours, ("h"), or days ("d") \
            Output filenames will have a suffix reflecting the rotation interval.
    :param interval: Deprecated: Interval multiplier for `with_rotation_time`, defaults to 1.
    :param verbose: enable debug logging
    :param cache_size: dataprofiles to cache
    :param segments: define either a list of segment keys or a list of segments tags: [  {"key":<featurename>,"value": <featurevalue>},... ]
    :param profile_full_dataset: when segmenting dataset, an option to keep the full unsegmented profile of the dataset.
    :param constraints: static assertions to be applied to streams and summaries.
    """

    def __init__(
        self,
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
        cache_size: int = 1,
        segments: Optional[Union[List[Segment], List[str]]] = None,
        profile_full_dataset: bool = False,
        constraints: DatasetConstraints = None,
    ):
        """"""
        self._active = True

        if session_timestamp is None:
            self.session_timestamp = datetime.datetime.now(datetime.timezone.utc)
        else:
            self.session_timestamp = session_timestamp
        self.dataset_name = dataset_name
        self.writers = writers
        self.verbose = verbose
        self.cache_size = cache_size
        self.tags = tags
        self.session_id = session_id
        self.metadata = metadata
        self.profile_full_dataset = profile_full_dataset
        self.constraints = constraints
        self.set_segments(segments)

        self._profiles = []
        self._intialize_profiles(dataset_timestamp)
        self.interval_multiplier = interval  # deprecated, rotation interval multiplier
        self.with_rotation_time = with_rotation_time  # rotation interval specification
        self._set_rotation(with_rotation_time)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def profile(
        self,
    ) -> DatasetProfile:
        """
        :return: the last backing dataset profile
        :rtype: DatasetProfile
        """
        return self._profiles[-1]["full_profile"]

    def tracking_checks(self):

        if not self._active:
            return False

        if self.should_rotate():
            self._rotate_time()
        return True

    @property
    def segmented_profiles(
        self,
    ) -> Dict[str, DatasetProfile]:
        """
        :return: the last backing dataset profile
        :rtype: Dict[str, DatasetProfile]
        """
        return self._profiles[-1]["segmented_profiles"]

    def get_segment(self, segment: Segment) -> Optional[DatasetProfile]:
        hashed_seg = hash_segment(segment)
        return self._profiles[-1]["segmented_profiles"].get(hashed_seg, None)

    def set_segments(self, segments: Union[List[Segment], List[str]]) -> None:
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

    def _intialize_profiles(
        self,
        dataset_timestamp: Optional[datetime.datetime] = datetime.datetime.now(datetime.timezone.utc),
    ) -> None:

        full_profile = None
        if self.full_profile_check():
            full_profile = DatasetProfile(
                self.dataset_name,
                dataset_timestamp=dataset_timestamp,
                session_timestamp=self.session_timestamp,
                tags=self.tags,
                metadata=self.metadata,
                session_id=self.session_id,
                constraints=self.constraints,
            )
        self._profiles.append({"full_profile": full_profile, "segmented_profiles": {}})

    def _set_rotation(self, with_rotation_time: str = None):
        if with_rotation_time is None:
            return

        self.with_rotation_time = with_rotation_time.lower()

        m = re.match(r"^(\d*)([smhd])$", with_rotation_time.lower())
        if m is None:
            raise TypeError("Invalid rotation interval, expected integer followed by one of 's', 'm', 'h', or 'd'")

        interval = 1 if m.group(1) == "" else int(m.group(1))
        if m.group(2) == "s":
            self.suffix = "%Y-%m-%d_%H-%M-%S"
        elif m.group(2) == "m":
            interval *= 60  # one minute
            self.suffix = "%Y-%m-%d_%H-%M"
        elif m.group(2) == "h":
            interval *= 60 * 60  # one hour
            self.suffix = "%Y-%m-%d_%H"
        elif m.group(2) == "d":
            interval *= 60 * 60 * 24  # one day
            self.suffix = "%Y-%m-%d"
        else:
            raise TypeError("Invalid rotation interval, expected integer followed by one of 's', 'm', 'h', or 'd'")
        # time in seconds
        current_time = int(datetime.datetime.utcnow().timestamp())
        self.interval = interval * self.interval_multiplier
        self.rotate_at = self.rotate_when(current_time)

    def rotate_when(self, time):
        return time + self.interval

    def should_rotate(
        self,
    ):

        if self.with_rotation_time is None:
            return False

        current_time = int(datetime.datetime.utcnow().timestamp())
        return current_time >= self.rotate_at

    def _rotate_time(self):
        """
        rotate with time add a suffix
        """
        current_time = int(datetime.datetime.utcnow().timestamp())
        # get the time that this current logging rotation started
        sequence_start = self.rotate_at - self.interval
        time_tuple = datetime.datetime.fromtimestamp(sequence_start)
        rotation_suffix = "." + time_tuple.strftime(self.suffix)
        log_datetime = datetime.datetime.strptime(time_tuple.strftime(self.suffix), self.suffix)

        # modify the segment datetime stamps
        if self.segments is None or self.profile_full_dataset:
            self._profiles[-1]["full_profile"].dataset_timestamp = log_datetime
        if self.segments is not None:
            for _, each_prof in self._profiles[-1]["segmented_profiles"].items():
                each_prof.dataset_timestamp = log_datetime

        self.flush(rotation_suffix)

        if len(self._profiles) > self.cache_size:
            self._profiles[-self.cache_size - 1] = None

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
                    writer.write(self._profiles[-1]["full_profile"], rotation_suffix)

            if self.segments is not None:

                for hashseg, each_seg_prof in self._profiles[-1]["segmented_profiles"].items():
                    seg_suffix = hashseg
                    full_suffix = "_" + seg_suffix
                    if rotation_suffix is not None:
                        full_suffix += rotation_suffix

                    writer.write(each_seg_prof, full_suffix)

    def full_profile_check(
        self,
    ) -> bool:
        """
        returns a bool to determine if unsegmented dataset should be profiled.
        """
        return (self.segments is None) or ((self.segments is not None) and self.profile_full_dataset)

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
    ):
        """
        Logs a collection of features or a single feature (must specify one or the other).

        :param features: a map of key value feature for model input
        :param feature_name: a dictionary of key->value for multiple features. Each entry represent a single columnar feature
        :param feature_name: name of a single feature. Cannot be specified if 'features' is specified
        :param value: value of as single feature. Cannot be specified if 'features' is specified

        """
        if not self.tracking_checks():
            return None

        if features is None and feature_name is None:
            return

        if features is not None and feature_name is not None:
            raise ValueError("Cannot specify both features and feature_name")

        if features is not None:
            # full profile
            self.log_dataframe(pd.DataFrame([features]))
        else:
            if self.full_profile_check():
                self._profiles[-1]["full_profile"].track_datum(feature_name, value)

            if self.segments:
                self.log_segment_datum(feature_name, value)

    def log_segment_datum(self, feature_name, value):
        segment = [{"key": feature_name, "value": value}]
        segment_profile = self.get_segment(segment)
        if self.segment_type == "keys":
            if feature_name in self.segments:
                if segment_profile is None:
                    return
                else:
                    segment_profile.track_datum(feature_name, value)
            else:
                for each_profile in self._profiles[-1]["segmented_profiles"]:
                    each_profile.track_datum(feature_name, value)
        elif self.segment_type == "set":
            if segment not in self.segments:
                return
            else:
                segment_profile.track_datum(feature_name, value)

    def log_metrics(
        self,
        targets,
        predictions,
        scores=None,
        model_type: ModelType = None,
        target_field=None,
        prediction_field=None,
        score_field=None,
    ):

        self._profiles[-1]["full_profile"].track_metrics(
            targets,
            predictions,
            scores,
            model_type=model_type,
            target_field=target_field,
            prediction_field=prediction_field,
            score_field=score_field,
        )

    def log_image(
        self,
        image,
        feature_transforms: Optional[List[Callable]] = None,
        metadata_attributes: Optional[List[str]] = METADATA_DEFAULT_ATTRIBUTES,
        feature_name: str = "",
    ):
        """
        API to track an image, either in PIL format or as an input path

        :param feature_name: name of the feature
        :param metadata_attributes: metadata attributes to extract for the images
        :param feature_transforms: a list of callables to transform the input into metrics
        :type image: Union[str, PIL.image]
        """
        if not self.tracking_checks():
            return None

        if isinstance(image, str):
            track_image = TrackImage(
                image,
                feature_transforms=feature_transforms,
                metadata_attributes=metadata_attributes,
                feature_name=feature_name,
            )
        else:
            track_image = TrackImage(
                img=image,
                feature_transforms=feature_transforms,
                metadata_attributes=metadata_attributes,
                feature_name=feature_name,
            )

        track_image(self._profiles[-1]["full_profile"])

    def log_local_dataset(
        self,
        root_dir,
        folder_feature_name="folder_feature",
        image_feature_transforms=None,
        show_progress=False,
    ):
        """
        Log a local folder dataset
        It will log data from the files, along with structure file data like
        metadata, and magic numbers. If the folder has single layer for children
        folders, this will pick up folder names as a segmented feature

        Args:
            root_dir (str): directory where dataset is located.
            folder_feature_name (str, optional): Name for the subfolder features, i.e. class, store etc.
            v (None, optional): image transform that you would like to use with the image log

        Raises:
            NotImplementedError: Description
        """
        try:
            from PIL.Image import Image as ImageType
        except ImportError as e:
            ImageType = None
            logger.debug(str(e))
            logger.debug("Unable to load PIL; install Pillow for image support")

        dst = LocalDataset(root_dir)
        for idx in tqdm(range(len(dst)), disable=(not show_progress)):
            # load internal and metadata from the next file
            ((data, magic_data), fmt), segment_value = dst[idx]

            # log magic number data if any, fmt, and folder name.
            self.log(feature_name="file_format", value=fmt)

            self.log(feature_name=folder_feature_name, value=segment_value)

            self.log(features=magic_data)

            if isinstance(data, pd.DataFrame):
                self.log_dataframe(data)

            elif isinstance(data, (Dict, list)):
                self.log_annotation(annotation_data=data)
            elif isinstance(data, ImageType):
                if image_feature_transforms:
                    self.log_image(
                        data,
                        feature_transforms=image_feature_transforms,
                        metadata_attributes=[],
                    )
                else:
                    self.log_image(data, metadata_attributes=[])
            else:
                raise NotImplementedError("File format not supported {}, format:{}".format(type(data), fmt))

    def log_annotation(self, annotation_data):
        """
        Log structured annotation data ie. JSON like structures


        Args:
            annotation_data (Dict or List): Description

        Returns:
            TYPE: Description
        """
        if not self.tracking_checks():
            return None
        track_bounding_box = TrackBB(obj=annotation_data)
        track_bounding_box(self._profiles[-1]["full_profile"])

    def log_csv(
        self,
        filepath_or_buffer: Union[str, Path, IO[AnyStr]],
        segments: Optional[Union[List[Segment], List[str]]] = None,
        profile_full_dataset: bool = False,
        **kwargs,
    ):
        """
        Log a CSV file. This supports the same parameters as :func`pandas.red_csv<pandas.read_csv>` function.

        :param filepath_or_buffer: the path to the CSV or a CSV buffer
        :type filepath_or_buffer: FilePathOrBuffer
        :param kwargs: from pandas:read_csv
        :param segments: define either a list of segment keys or a list of segments tags: `[  {"key":<featurename>,"value": <featurevalue>},... ]`
        :param profile_full_dataset: when segmenting dataset, an option to keep the full unsegmented profile of the
        dataset.
        """

        self.profile_full_dataset = profile_full_dataset
        if segments is not None:
            self.set_segments(segments)

        df = pd.read_csv(filepath_or_buffer, **kwargs)
        self.log_dataframe(df)

    def log_dataframe(
        self,
        df,
        segments: Optional[Union[List[Segment], List[str]]] = None,
        profile_full_dataset: bool = False,
    ):
        """
        Generate and log a whylogs DatasetProfile from a pandas dataframe
        :param profile_full_dataset: when segmenting dataset, an option to keep the full unsegmented profile of the
         dataset.
        :param segments: specify the tag key value pairs for segments
        :param df: the Pandas dataframe to log
        """
        if not self.tracking_checks():
            return None

        # segment check  in case segments are just keys
        self.profile_full_dataset = profile_full_dataset
        if segments is not None:
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
                segment_tags = [{"key": self.segments[i], "value": each_segment[i]} for i in range(len(self.segments))]

                self.log_df_segment(segment_df, segment_tags)
            except KeyError:
                continue

    def log_fixed_segments(self, data):
        # we group each segment seperately since the segment tags are allowed
        # to overlap
        for segment_tag in self.segments:
            # create keys
            segment_keys = [feature["key"] for feature in segment_tag]
            seg = tuple(feature["value"] for feature in segment_tag)

            grouped_data = data.groupby(segment_keys)

            if len(seg) == 1:
                seg = seg[0]
            # check if segment exist
            if seg not in grouped_data.groups:
                continue
            segment_df = grouped_data.get_group(seg)

            self.log_df_segment(segment_df, segment_tag)

    def log_df_segment(self, df, segment: Segment):
        segment = sorted(segment, key=lambda x: x["key"])

        segment_profile = self.get_segment(segment)

        if segment_profile is None:
            segment_profile = DatasetProfile(
                self.dataset_name,
                dataset_timestamp=datetime.datetime.now(datetime.timezone.utc),
                session_timestamp=self.session_timestamp,
                tags={**self.tags, **{"segment": json.dumps(segment)}},
                metadata=self.metadata,
                session_id=self.session_id,
                constraints=self.constraints,
            )
            segment_profile.track_dataframe(df)
            hashed_seg = hash_segment(segment)
            self._profiles[-1]["segmented_profiles"][hashed_seg] = segment_profile
        else:
            segment_profile.track_dataframe(df)

    def is_active(self):
        """
        Return the boolean state of the logger
        """
        return self._active


def hash_segment(seg: List[Dict]) -> str:
    return hashlib.sha256(json.dumps(seg).encode("utf-8")).hexdigest()
