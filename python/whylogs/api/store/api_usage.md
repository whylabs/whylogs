```python
import whylogs as why
from whylogs.api.store import ProfileStore


key = ProfileKey(dataset_id="some_id_123", timestamp=None) # infers timestamp from datetime.today()

# 1. Initialize the service
store = ProfileStore(key=) # or S3Store, etc.
validator = ProfileValidator(condition=my_condition, callback=my_cb)
logger = why.logger(mode="rolling", store=store)

# 2.  make predictions and log them
pred, metrics = model.predict(df)
logger.log(pred)  # -> triggers logger.store.write()
logger.log(metrics)

logger.validator.run(df, store=store)  # validate ProfileStore._merged_profile against ProfileStore.write(df)
```
