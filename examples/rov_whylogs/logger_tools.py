import pandas as pd

from whylogs import get_or_create_session


def log_session(dataset_name, session_data):
    session = get_or_create_session()
    df = pd.DataFrame(session_data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df_minutes = df.groupby(pd.Grouper(key="timestamp", freq="min"))
    for minute_batch, batch in df_minutes:
        with session.logger(dataset_name=dataset_name, dataset_timestamp=minute_batch) as logger:
            logger.log_dataframe(batch)
