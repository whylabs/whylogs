import json
import os
import time
from collections import OrderedDict
from logging import getLogger as _getLogger

import s3fs
from google.protobuf.message import Message

from whylabs.logs.util import datapath


class S3Handler:
    """
    Handle output to S3
    """
    def __init__(self, bucket=None, prefix=None, team=None, project=None,
                 pipeline=None, user=None, **kwargs):
        if bucket is None:
            raise ValueError("bucket not sets")
        self.bucket = bucket
        self.dest_config = OrderedDict(
            prefix=prefix,
            team=team,
            project=project,
            pipeline=pipeline,
            user=user,
        )
        self._logger = _getLogger(__name__)
        # TODO: not sure if this should get initialized one time at __init__
        # or whenever requested
        self._s3filesystem = s3fs.S3FileSystem()

    def _base_directory(self, **kwargs):
        """
        Parameters
        ----------
        kwargs :
            Override DiskHandler constructor kwargs
        """
        if kwargs.get('prefix', None) is not None:
            kwargs['prefix'] = os.path.realpath(kwargs['prefix'])
        opts = OrderedDict()
        opts.update(self.dest_config)
        opts.update(kwargs)
        parts = [v for v in opts.values() if v is not None]
        directory = '/'.join(parts)
        return directory

    def _init_output(self, fmt, ext, logger_name, object_type, timestamp_ms,
                     dest_kw={}):
        """
        Generate an output file name and create the containing folder.
        """
        if timestamp_ms is None:
            timestamp_ms = 1000 * time.time()
        timestamp_ms = int(timestamp_ms)
        folder = self.directory(logger_name, object_type, fmt, **dest_kw)
        key = f'{object_type}-{timestamp_ms}{ext}'
        key = '/'.join([folder, key])
        path = datapath.CloudPath(key, bucket=self.bucket)

        return path

    def directory(self, logger_name, object_type, format, **kwargs):
        """
        Parameters
        ----------
        logger_name : str
            Name of the logger
        object_type : str
            The kind of object we're logging, e.g. dataset_summary or
            dataset_profile
        format : str
            Output format
        kwargs :
            Override DiskHandler constructor kwargs
        """
        base = self._base_directory(**kwargs)
        return '/'.join([base, logger_name, object_type, format])

    def handle_protobuf(self, x: Message, logger_name, object_type,
                        timestamp_ms, dest_kw={}):
        """
        Log a protobuf message
        """
        path = self._init_output('protobuf', '.bin', logger_name, object_type,
                                  timestamp_ms, dest_kw)
        uri = path.uri
        msg = x.SerializeToString()
        with self._s3filesystem.open(uri, 'wb') as fp:
            self._logger.debug('Writing protobuf file %s', uri)
            fp.write(msg)
        return HandlerResponse(dest=path.uri)

    def handle_json(self, x: str, logger_name, object_type, timestamp_ms,
                    dest_kw={}):
        path = self._init_output('json', '.json', logger_name, object_type,
                                  timestamp_ms, dest_kw)
        uri = path.uri
        with self._s3filesystem.open(uri, 'wt') as fp:
            self._logger.debug('Writing JSON file %s', uri)
            fp.write(x)
        return HandlerResponse(uri)

    def handle_flat(self, x: str, logger_name, object_type, timestamp_ms,
                    dest_kw={}):
        if object_type == 'dataset_summary':
            return self._handle_flat_dataset_summary(
                x, logger_name, timestamp_ms, dest_kw)
        else:
            raise ValueError(f'Unsupported flat object type: {object_type}')

    def _handle_flat_dataset_summary(self, x: dict, logger_name, timestamp_ms,
                                     dest_kw={}):
        # Initialize
        self._logger.debug('Writing flat dataset summary')
        if timestamp_ms is None:
            timestamp_ms = int(1000 * time.time())
        otype = 'dataset_summary'
        destinations = {}

        # Write flat table
        df = x['summary']
        fmt = 'flat_table'
        path = self._init_output(fmt, '.csv', logger_name,  otype,
                                  timestamp_ms, dest_kw)
        uri = path.uri
        self._logger.debug('Writing dataset summary table to %s', uri)
        df.to_csv(uri, index=False)
        destinations[fmt] = uri

        # Write histograms
        # TODO: use self.handle_json
        hist = x['hist']
        fmt = 'histogram'
        path = self._init_output(fmt, '.json', logger_name, otype,
                                  timestamp_ms, dest_kw)
        uri = path.uri
        self._logger.debug('Writing dataset histograms to %s', uri)
        json.dump(hist, self._s3filesystem.open(uri, 'wt'))
        destinations[fmt] = uri

        # Write frequent strings
        # TODO: use self.handle_json
        freq_strings = x['frequent_strings']
        fmt = 'freq_strings'
        path = self._init_output(fmt, '.json', logger_name,
                                  otype, timestamp_ms, dest_kw)
        uri = path.uri
        self._logger.debug('Writing dataset frequent strings to %s', uri)
        json.dump(freq_strings, self._s3filesystem.open(uri, 'wt'))
        destinations[fmt] = uri
        return HandlerResponse(dest=destinations)


class DiskHandler:
    """
    Handle output to disk
    """
    def __init__(self, folder=None, team=None, project=None, pipeline=None,
                 user=None, **kwargs):
        if folder is None:
            folder = '.'
        folder = os.path.realpath(folder)
        self.dest_config = OrderedDict(
            folder=folder,
            team=team,
            project=project,
            pipeline=pipeline,
            user=user,
        )
        self._logger = _getLogger(__name__)

    def _base_directory(self, **kwargs):
        """
        Parameters
        ----------
        kwargs :
            Override DiskHandler constructor kwargs
        """
        if kwargs.get('folder', None) is not None:
            kwargs['folder'] = os.path.realpath(kwargs['folder'])
        opts = OrderedDict()
        opts.update(self.dest_config)
        opts.update(kwargs)
        parts = [v for v in opts.values() if v is not None]
        directory = os.path.join(*parts)
        return directory

    def _init_output(self, fmt, ext, logger_name, object_type, timestamp_ms,
                     dest_kw={}):
        """
        Generate an output file name and create the containing folder.
        """
        if timestamp_ms is None:
            timestamp_ms = 1000 * time.time()
        timestamp_ms = int(timestamp_ms)
        folder = self.directory(logger_name, object_type, fmt, **dest_kw)
        os.makedirs(folder, exist_ok=True)
        fname = f'{object_type}-{timestamp_ms}{ext}'
        fname = os.path.join(folder, fname)
        return fname

    def directory(self, logger_name, object_type, format, **kwargs):
        """
        Parameters
        ----------
        logger_name : str
            Name of the logger
        object_type : str
            The kind of object we're logging, e.g. dataset_summary or
            dataset_profile
        format : str
            Output format
        kwargs :
            Override DiskHandler constructor kwargs
        """
        base = self._base_directory(**kwargs)
        return os.path.join(base, logger_name, object_type, format)

    def handle_protobuf(self, x: Message, logger_name, object_type,
                        timestamp_ms, dest_kw={}):
        """
        Log a protobuf message to disk
        """
        fname = self._init_output('protobuf', '.bin', logger_name, object_type,
                                  timestamp_ms, dest_kw)
        msg = x.SerializeToString()
        with open(fname, 'wb') as fp:
            self._logger.debug('Writing protobuf file %s', fname)
            fp.write(msg)
        return HandlerResponse(dest=fname)

    def handle_json(self, x: str, logger_name, object_type, timestamp_ms,
                    dest_kw={}):
        fname = self._init_output('json', '.json', logger_name, object_type,
                                  timestamp_ms, dest_kw)
        with open(fname, 'wt') as fp:
            self._logger.debug('Writing JSON file %s', fname)
            fp.write(x)
        return HandlerResponse(fname)

    def handle_flat(self, x: str, logger_name, object_type, timestamp_ms,
                    dest_kw={}):
        if object_type == 'dataset_summary':
            return self._handle_flat_dataset_summary(
                x, logger_name, timestamp_ms, dest_kw)
        else:
            raise ValueError(f'Unsupported flat object type: {object_type}')

    def _handle_flat_dataset_summary(self, x: dict, logger_name, timestamp_ms,
                                     dest_kw={}):
        # Initialize
        self._logger.debug('Writing flat dataset summary')
        if timestamp_ms is None:
            timestamp_ms = int(1000 * time.time())
        otype = 'dataset_summary'
        destinations = {}

        # Write flat table
        df = x['summary']
        fmt = 'flat_table'
        fname = self._init_output(fmt, '.csv', logger_name,  otype,
                                  timestamp_ms, dest_kw)
        self._logger.debug('Writing dataset summary table to %s', fname)
        df.to_csv(fname, index=False)
        destinations[fmt] = fname

        # Write histograms
        # TODO: use self.handle_json
        hist = x['hist']
        fmt = 'histogram'
        fname = self._init_output(fmt, '.json', logger_name, otype,
                                  timestamp_ms, dest_kw)
        self._logger.debug('Writing dataset histograms to %s', fname)
        json.dump(hist, open(fname, 'wt'))
        destinations[fmt] = fname

        # Write frequent strings
        # TODO: use self.handle_json
        freq_strings = x['frequent_strings']
        fmt = 'freq_strings'
        fname = self._init_output(fmt, '.json', logger_name,
                                  otype, timestamp_ms, dest_kw)
        self._logger.debug('Writing dataset frequent strings to %s', fname)
        json.dump(freq_strings, open(fname, 'wt'))
        destinations[fmt] = fname
        return HandlerResponse(dest=destinations)


class HandlerResponse:
    def __init__(self, dest=None, success=True):
        self.dest = dest
        self.success = success