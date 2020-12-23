import pandas as pd
import time
from whylogs.core.datasetprofile import dataframe_profile
from whylogs import get_or_create_session

if __name__ == "__main__":
    df = pd.read_csv("data/lending-club-accepted-10.csv")
    
    session = get_or_create_session()
    profile = session.log_dataframe(df,'test.data')
    summary = profile.flat_summary()
    time.sleep(2)
    profile = session.log_dataframe(df,'test.data')
    summary = profile.flat_summary()
    time.sleep(5)
    profile = session.log_dataframe(df,'test.data')
    summary = profile.flat_summary()
    flat_summary = summary['summary']
    print(flat_summary)

