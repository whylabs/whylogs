from tqdm import tqdm
from pmdarima.datasets import load_msft
import pandas as pd
from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import MultiDatasetRollingLogger
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import TimeGranularity


df = load_msft()

df['Date'] = pd.to_datetime(df['Date'])
groups = df.set_index('Date').groupby(pd.Grouper(freq='Y'))

logger = MultiDatasetRollingLogger(aggregate_by=TimeGranularity.Year)
for row in tqdm(df.to_dict(orient="records")): 
    timestamp_ms = int(row['Date'].timestamp() * 1000)
    logger.log(row, timestamp_ms=timestamp_ms, sync=True)


results = logger.get_results()


top_three = sorted(results.items(), reverse=True)[:3]
top_three_results = [result.view() for ts, result in top_three]
print(top_three_results )

logger.close()