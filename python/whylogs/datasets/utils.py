from datetime import date, datetime

from numpy import isin


def _adjust_date(current_date, date_offset):
    adjusted_date = datetime.strptime(current_date, "%Y-%m-%d").date() + date_offset

    return adjusted_date


def _change_df_date_by_offset(df, new_start_date):
    original_start_date = df["date"][0]

    if isinstance(original_start_date, str):
        original_start_date = datetime.strptime(original_start_date, "%Y-%m-%d").date()
        date_offset = new_start_date - original_start_date
        df["date"] = df["date"].apply(lambda x: _adjust_date(x, date_offset))
        return df

    elif not isinstance(original_start_date, (date, datetime)):
        raise ValueError("Date column must be either valid string format, date or datetime types.")

    date_offset = new_start_date - original_start_date
    df["date"] = df["date"].apply(lambda x: x + date_offset)
    return df


def _validate_timestamp(timestamp):
    if isinstance(timestamp, datetime):
        return timestamp.date()
    elif isinstance(timestamp, date):
        return timestamp
    else:
        raise ValueError("You must pass either a Datetime or Date object to timestamp!")
