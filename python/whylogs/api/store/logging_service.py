from datetime import datetime, timedelta
from functools import reduce
from glob import glob

import whylogs as why


def _get_date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def _get_dates_list(start_date: datetime, end_date: datetime):
    dates_list = []
    for single_date in _get_date_range(start_date, end_date):
        dates_list.append(single_date.strftime("%Y-%m-%d"))
    return dates_list


def read_profiles(base_name: str, start_date: datetime, end_date: datetime):
    dates_list = _get_dates_list(start_date=start_date, end_date=end_date)
    files_list = []
    for date in dates_list:
        files_list.extend(glob(f"{base_name}.{date}*.bin"))
    profiles_list = [why.read(file).view() for file in files_list]
    merged_profile = reduce(lambda x, y: x.merge(y), profiles_list)
    return merged_profile


# import pandas as pd
# import whylogs as why

# # create a 5-minutely rolling logger
# logger = why.logger(mode="rolling", interval=10, when="S", base_name="test_base_name")
# # add a local writer
# logger.append_writer("local", base_dir="whylogs_output")


# def predict(df: pd.DataFrame):
#     logger.log(df)
#     new_df = pd.DataFrame([[1,2,3]], columns=["aa","bb","cc"])
#     logger.log(new_df)


# if __name__ == "__main__":
#     for i in range(30):
#         predict(pd.DataFrame([[i,2]], columns=["a","b"]))
#         time.sleep(1)
#     logger.close()
