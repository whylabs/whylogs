"""
"""
from logging import getLogger as _getLogger
import pandas as pd
import os
from collections import OrderedDict
import time
import json

from google.protobuf.message import Message

from whylabs.logs.app import config as app_config
from whylabs.logs.core import datasetprofile
from whylabs.logs.util.protobuf import message_to_json
from whylabs.logs.util import datapath
import s3fs


class Session:
    """
    Parameters
    ----------
    activate : bool (default=True)
        Activate the returned session.  If False, the **kwargs must be empty!
    **kwargs :
        Session configuration, passed to `whylabs.logs.app.config.load_config`
    """
    def __init__(self, activate=True, **kwargs):
        self.reset()
        if activate:
            self.activate()
        else:
            if len(kwargs) > 0:
                raise ValueError(
                    "Cannot parse session kwargs with activate=False")

    def reset(self):
        print('RESETTING')
        self._active = False
        self.config = None
        if hasattr(self, 'logger'):
            self.logger.reset()
        else:
            self.logger = Logger()

    def activate(self, **kwargs):
        """
        Load config and activate the session
        """
        self._init_config = kwargs
        self.config = app_config.load_config(**kwargs)
        self.logger.set_session_config(self.config)
        self._active = True

    def is_active(self):
        return self._active


class Logger:
    """
    Class for logging WhyLogs statistics.

    TODO: logger overrides for session config

    Parameters
    ----------
    session_config : dict
        Controls config for the logging at the session level.  If specified,
        the logger will be activated.
    """
    def __init__(self, session_config=None):
        self.reset()
        self.set_session_config(session_config)

    def reset(self):
        self.set_session_config(None)

    @property
    def _config(self):
        return self._session_config

    def _init_handlers(self):
        config = self._config

        user_params = {
            'project': config['project'],
            'pipeline': config['pipeline'],
            'user': config['user'],
            'team': config['team'],
        }
        handler_configs, destinations = app_config._get_handler_configs(config)
        self._handler_configs = handler_configs
        self._handlers = {}
        if 's3' in destinations:
            self._handlers['s3'] = S3Handler(
                bucket=config['bucket'],
                prefix=config['cloud_output_folder'],
            )
        if 'stdout' in destinations:
            raise NotImplementedError('stdout output not implemented')
        if 'disk' in destinations:
            self._handlers['disk'] = DiskHandler(
                folder=config['local_output_folder'],
                **user_params,
            )
        self._destinations = destinations
        self._output_steps = app_config._get_output_steps(
            self._handler_configs)
        self._user_params = user_params

    def set_session_config(self, session_config: dict=None):
        """
        Set config defined by a session and activate the logger if the
        session is active.
        """
        from copy import deepcopy
        if session_config is None:
            self._session_config = None
            self._active = False
        else:
            self._session_config = deepcopy(session_config)
            self._active = True
            self._init_handlers()

    def log_dataset_profile(self, profile: datasetprofile.DatasetProfile,
                            name=None):
        """
        Log a dataset profile

        Parameters
        ----------
        profile : DatasetProfile
            The profile to log
        name : str
            The name of the item being logged, e.g. 'training' or
            'training.data'

        Returns
        -------
        response : dict
            A dictionary response, currently containing:
            * 'handler_responses' - a list of responses from the output
                handlers
        """
        if not self.is_active():
            return
        timestamp_ms = int(1000 * time.time())
        if name is None:
            name = 'whylogs'
        steps = self._output_steps

        responses = []

        if 'dataset_profile' in steps:
            fmts = steps['dataset_profile']
            for fmt, destinations in fmts.items():
                if fmt == 'protobuf':
                    msg = profile.to_protobuf()
                    for dest in destinations:
                        r = self._handlers[dest].handle_protobuf(
                            msg, name, 'dataset_profile', timestamp_ms)
                        responses.append({'fmt': fmt, 'dest': dest,
                                          'response': r})
                else:
                    raise ValueError(
                        f'Unrecognized dataset_profile format: {fmt}')

        if 'dataset_summary' in steps:
            summary = profile.to_summary()
            fmts = steps['dataset_summary']
            for fmt, destinations in fmts.items():
                if fmt == 'flat':
                    flat = datasetprofile.flatten_summary(summary)
                    for dest in destinations:
                        r = self._handlers[dest].handle_flat(
                            flat, name, 'dataset_summary', timestamp_ms)
                        responses.append({'fmt': fmt, 'dest': dest,
                                          'response': r})
                elif fmt == 'json':
                    x_json = message_to_json(summary)
                    for dest in destinations:
                        r = self._handlers[dest].handle_json(
                            x_json, name, 'dataset_summary', timestamp_ms)
                        responses.append({'fmt': fmt, 'dest': dest,
                                          'response': r})
                else:
                    raise ValueError(
                        f'Unrecognized dataset_summary format: {fmt}'
                    )
        return {'handler_responses': responses}

    def log_dataframe(self, df: pd.DataFrame, name=None):
        """
        Generate and log a WhyLogs DatasetProfile from a pandas dataframe

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to log
        name : str
            Name of the dataset, e.g. 'training.data'
        """
        if not self.is_active():
            return
        profile = datasetprofile.dataframe_profile(df, name)
        out = self.log_dataset_profile(profile, name)
        out['profile'] = profile
        return out

    def is_active(self):
        return self._active


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


# Create a global session
_session = Session(activate=False)


def reset_session():
    """
    Reset and inactivate the logging session.
    """
    global _session
    _session.reset()


def get_or_create_session(**kwargs):
    """
    Retrieve the current active session.  If no active session is found,
    create the session.

    Parameters
    ----------
    kwargs:
        Session configuration, passed to the session.  These are ignored
        if an active session is already found.

    Returns
    -------
    session : Session
        The active session
    """
    global _session
    if _session.is_active():
        _getLogger(__name__).debug(
            'Active session found, ignoring session kwargs')
    else:
        _session.activate(**kwargs)
    return _session


def get_session():
    """
    Retrieve the logging session without altering or activating it.
    """
    return _session


def get_logger():
    """
    Retrieve the logger.
    """
    return _session.logger
