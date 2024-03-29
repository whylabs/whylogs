# Profiling

whylogs approaches data logging through the process of __Profiling__. With profiling, logs are generated by collecting statistical measurements of the data. These profiles are scalable, lightweight, flexible, and configurable.

Using this approach requires only a single pass over the data with minimal memory overhead, and is naturally parallelizable. The resulting profiles are all mergeable, allowing statistics from multiple hosts, data partitions, or datasets, to be merged post-hoc.

Unlike sampling methods, profiling allows you to accurately capture rare events and outlier-dependent metrics.

If you want to know more about the differences between sampling and profiling, check out the [Data Logging: Sampling versus Profiling](https://whylabs.ai/blog/posts/data-logging-sampling-versus-profiling) blog post!

## Generating Profiles

Generating a profile from a dataframe can be done with one line of code, like shown below:

```python
import pandas as pd
data = {
    "animal": ["cat", "hawk", "snake", "cat"],
    "legs": [4, 2, 0, 4],
    "weight": [4.3, 1.8, 1.3, 4.1],
}

df = pd.DataFrame(data)

import whylogs as why

profile = why.log(df).profile()
```

Make sure to check [our examples](../examples.rst) to see what you can do with the generated profiles!
