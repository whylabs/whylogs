"""
Define classes for interacting with whylogs data paths
"""
import os
import re
from copy import deepcopy
from datetime import datetime, timedelta

DEFAULT_CLOUD_PROVIDER = "s3"
CLOUD_PROVIDERS = ("s3",)


def parse_uri(x: str):
    """
    Parse an URI path

    Parameters
    ----------
    x : str
        URI

    Returns
    -------
    cloud_provider : str, None
        The cloud provider, e.g. s3.  None if a local path
    bucket : str
        The remote storage bucket, None if a local path
    key : str
        The object key (if remote) or path (if local)
    """
    parts = x.split("://")
    key = None
    bucket = None
    cloud_provider = None
    if len(parts) == 1:
        # No cloud provider, assume there's no bucket
        key = x
    elif len(parts) == 2:
        cloud_provider = parts[0]
        if cloud_provider not in CLOUD_PROVIDERS:
            raise ValueError(f"Unrecognized cloud provider: {cloud_provider}")
        bucket, key = parts[1].split("/", maxsplit=1)
    else:
        raise ValueError("Cannot parse key")
    return cloud_provider, bucket, key


class CloudPath:
    """
    A path interface which tracks the key, bucket, and cloud provider (e.g.
    S3) for a cloud data path or a local path

    Parameters
    ----------
    key : str
        Key (for remote, object store data) or the path (for local storage)
    bucket : str, None
        For remote object store data, specifies the data path.  Set to
        `None` for local data
    cloud_proivider : str, None
        Specify the cloud provider, e.g. 's3'.  Set to `None` for local data.
    """

    def __init__(self, key, bucket=None, cloud_provider=None):
        self.bucket = bucket
        self.key = key
        self.cloud_provider = cloud_provider

    @property
    def cloud_provider(self):
        return self._cloud_provider

    @cloud_provider.setter
    def cloud_provider(self, x):
        if x is None:
            x = DEFAULT_CLOUD_PROVIDER
        self._cloud_provider = x

    @property
    def uri(self):
        if self.bucket is None:
            return self.key
        else:
            return f"{self.cloud_provider}://{self.bucket}/{self.key}"

    @staticmethod
    def from_uri(x: str):
        parts = x.split("://")
        if len(parts) == 1:
            # No cloud provider, assume there's no bucket
            return CloudPath(x)
        elif len(parts) == 2:
            cloud_provider = parts[0]
            if cloud_provider not in CLOUD_PROVIDERS:
                raise ValueError(f"Unrecognized cloud provider: {cloud_provider}")
            bucket, key = parts[1].split("/", maxsplit=1)
            return CloudPath(key, bucket, cloud_provider)
        else:
            raise ValueError("Cannot parse key")


class DataPath:
    """
    Key format:

        <cloud_provider>://<bucket>/<prefix>/customer/pipeline/dataset/format/year/day/hour/<name>-<t (s)><ext>


    """

    def __init__(
        self,
        cloud_provider=None,
        bucket=None,
        customer=None,
        pipeline=None,
        dataset=None,
        format=None,
        t_ns=None,
        t=None,
        name="",
        prefix=None,
        ext="",
    ):
        self._timestamp_path_info = dict(
            t_ns=t_ns, t=t, name=name, prefix=prefix, ext=ext
        )
        self.cloud_provider = cloud_provider
        self.bucket = bucket
        self.customer = customer
        self.pipeline = pipeline
        self.dataset = dataset
        self.format = format

    @property
    def timestamped_data_path(self):
        return ...

    @property
    def prefix(self):
        raise NotImplementedError


class TimestampedDataPath:
    """
    A simple class to handle filenames for objects in data sets stored on S3

    Keys are formatted according to:

        # On the cloud
        <cloud_provider>://<bucket>/<prefix>/year/day/hour/<name>-<t (ns)><ext>

        # Local files (no bucket)
        <prefix>/year/day/hour/<name>-<t (ns)><ext>

    You can initialize a TimestampedDataPath or create one from a key with
    TimestampedDataPath.from_key(key)

    Parameters
    ----------
    t_ns : int
        UTC timestamp in nanoseconds.  One of t, or t_ns must be supplied.
        t_ns takes precedence over t
    t : float
        UTC timestamp in seconds.
    name : str, optional
        Name for the file (<name> above)
    prefix : str, optional
        Folder-like prefix
    ext : optional
        File extension to use, with the leading period

    Attributes
    ----------
    Attributes are dynamically generated.

    t, name, prefix, ext (listed above)

    key : str
        Automatically generated key
    time_folder : str
        The folder string year/day/hour

    Examples
    --------
    .. code-block:: python

        # Parse a key
        path = TimestampedDataPath.from_key(key)
        print(path.t)
        print(path.key)

        # Parse a URI
        path = Timestamped.from_uri(key)

        # Change parameters
        print(path.key)
        path.t = 1234
        print(path.key)
        path.bucket = 'new_bucket'
        print(path.uri)

        # Construct a DataPath
        path = TimestampedDataPath(t=time.time(), name='filename',
                prefix='cloud/path/prefix', ext='.json')
    """

    def __init__(
        self,
        t_ns=None,
        t=None,
        bucket=None,
        cloud_provider=None,
        name="",
        prefix="",
        ext="",
    ):
        if t_ns is None:
            if t is None:
                raise ValueError("Must supply t or t_ns")
            self.t = t
        else:
            self.t_ns = t_ns

        self.prefix = prefix
        self.name = name
        self.ext = ext
        self.bucket = bucket
        self.cloud_provider = cloud_provider

    def copy(self):
        return deepcopy(self)

    @property
    def cloud_provider(self):
        return self._cloud_provider

    @cloud_provider.setter
    def cloud_provider(self, x):
        if x is None:
            x = DEFAULT_CLOUD_PROVIDER
        self._cloud_provider = x

    @property
    def uri(self):
        if self.bucket is None:
            return self.key
        else:
            return f"{self.cloud_provider}://{self.bucket}/{self.key}"

    @property
    def key(self):
        t_ns = self.t_ns
        fname = self.name + "-" + str(t_ns) + self.ext
        return os.path.join(self.prefix, self.time_folder, fname)

    @property
    def t(self):
        return 1e-9 * self.t_ns

    @t.setter
    def t(self, x):
        self.t_ns = int(x * 1e9 + 0.5)

    @property
    def time_folder(self):
        return utc_folder(self.t)

    @property
    def filename(self):
        return self.name + "-" + str(self.t_ns) + self.ext

    @staticmethod
    def from_uri(x: str):
        cloud_provider, bucket, key = parse_uri(x)
        p = TimestampedDataPath.from_key(key, bucket, cloud_provider)
        return p

    @staticmethod
    def from_key(key, bucket=None, cloud_provider=None):
        folder, basename = os.path.split(key)
        # Try to find a timestamp
        t_ns = None
        t = None
        try:
            t_str = re.findall(r"\D(\d{19})\D", " " + basename + " ")[-1]
            t_ns = int(t_str)
        except IndexError:
            try:
                t_str = re.findall(r"\D(\d{10})\D", " " + basename + " ")[-1]
                t = float(t_str)
            except IndexError:
                raise ValueError("No timestamp found")

        t_ind = basename.rfind(t_str)
        ext = basename[t_ind + len(t_str) :]
        name = basename[0 : t_ind - 1]
        # Get the dataset prefix.  The last 3 folder levels are the file path
        folders = folder.split("/")
        prefix = "/".join(folders[0:-3])

        return TimestampedDataPath(
            t_ns=t_ns,
            t=t,
            name=name,
            prefix=prefix,
            ext=ext,
            bucket=bucket,
            cloud_provider=cloud_provider,
        )


def utc_folder(x):
    """
    Convert a UTC timestamp to a folder string
    """
    return datetime.utcfromtimestamp(x).strftime("%Y/%j/%H")


def utc_timestamp(year=2019, month=None, day=1, hour=0, minute=0, second=0):
    """
    Create a UTC timestamp

    By Default, day is day of the year unless you specify a month

    Parameters
    ----------
    year : int
        Year
    month : int, optional
        IF specified, month of year.  If NOT specified, day will specify the
        day of the year (not the day of the month).  Default = None
    day : int
        Day of the year if month is not specified, otherwise day of the month
    hour : int
        Hour of day
    minute : int
        Minute of hour
    second : float
        Seconds.

    Returns
    -------
    t : float
        UTC timestamp
    """

    microsecond = int(1e6 * (second - int(second)))
    second = int(second)
    year = int(year)
    day = int(day)
    hour = int(hour)
    minute = int(minute)

    import pytz

    if month is None:
        dt = datetime(year, 1, 1, hour, minute, second, microsecond, tzinfo=pytz.utc)
        dt = dt + timedelta(day - 1)
    else:
        dt = datetime(
            year, month, day, hour, minute, second, microsecond, tzinfo=pytz.utc
        )

    return dt.timestamp()
