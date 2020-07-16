import time

import pandas as pd

from whylabs.logs.app import config as app_config
from whylabs.logs.app.handlers import S3Handler, DiskHandler
from whylabs.logs.core import datasetprofile
from whylabs.logs.util.protobuf import message_to_json


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