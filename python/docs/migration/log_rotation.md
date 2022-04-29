# Log Rotation

```python
import pandas as pd
import whylogs as why

# create a 5-minutely rolling logger
logger = why.logger(mode="rolling", interval=5, when="M", base_name="test_base_name")
# add a local writer
logger.append_writer("local", base_dir="whylogs_output")


def predict(df: pd.DataFrame):
    logger.log(df)
    res = model.predict(df)
    logger.log(res)

```
