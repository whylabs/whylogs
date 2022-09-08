# import asyncio
# import time
# from cProfile import Profile
# from dataclasses import dataclass
# from datetime import datetime
# from functools import reduce
# from typing import Optional

# import whylogs as why
# from whylogs.core import DatasetProfileView
# from whylogs.core.stubs import _pd as pd

# DEFAULT_PATH = "location/"
# DEFAULT_MERGE_PERIOD = 5 * 60


# @dataclass
# class ProfileKey:
#     timestamp: datetime
#     project_id: str
#     # TODO add segment ?


# class ProfileStore:
#     def __init__(self, reference_date_start, reference_date_end, reference_key) -> None:
#         self._loop = asyncio.new_event_loop()

#         # TODO start coroutine on another thread
#         asyncio.run_coroutine_threadsafe(self.merge_profiles(date=datetime.now()), self._loop)

#         self._ref_profile = self.get_reference(
#             start_date=reference_date_start, end_date=reference_date_end, profile_key=reference_key
#         )

#     @asyncio.coroutine
#     def merge_profiles(self, date: datetime) -> None:
#         while True:
#             profiles = why.read(DEFAULT_PATH, date)
#             merged_profile = yield reduce(lambda x, y: x.merge(y), profiles)
#             self._merged_profile = merged_profile
#             merged_profile.writer("local").write(DEFAULT_PATH + "merged/")
#             time.sleep(DEFAULT_MERGE_PERIOD)

#     def get_profile(self, start_date, end_date, key: ProfileKey) -> DatasetProfileView:
#         pass

#     def get_merged_profile(self, start_date, end_date, key: ProfileKey) -> DatasetProfileView:
#         # Loosely define a merged profile, return an Empty one if merged not already found
#         try:
#             return why.read(DEFAULT_PATH + "merged/")
#         except FileNotFoundError:
#             return DatasetProfileView()

#     def get_reference(
#         self, start_date: datetime, end_date: datetime, window: str, key: ProfileKey
#     ) -> DatasetProfileView:
#         # 1. Fixed profile from date
#         # 2. Fixed window, moving profile -> TODO how often to update?
#         pass

#     def write_profile(self, dataframe: Optional[pd.DataFrame], profile_view: Optional[DatasetProfileView]) -> None:
#         # TODO always write? or should this be in memory and only merged from time to time?
#         if dataframe is not None:
#             profile = why.log(dataframe)
#             profile.writer("local").write(path=DEFAULT_PATH)
#         elif profile_view is not None:
#             profile_view.write(dest=DEFAULT_PATH)
#         else:
#             raise ValueError("You must define either a dataframe or profile view")

#     def close(self):
#         self.merge_profiles.close()
