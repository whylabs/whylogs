# FAQs

## Technical

### What is Data Logging?

Unlike traditional logging which is standard practice in all software systems, data logging is a practice necessary for ML/AI applications. Data logging involves creating a log of statistical properties of the data that flows through an ML/AI application. Checkout our [blogs on parallels between DevOps and MLOps logging approaches](https://medium.com/whylabs/whylogs-embrace-data-logging-a9449cd121d) and on the practice of data logging in data science.


### Can you handle large scale data?

whylogs is built for scale and optimized for massive data sets. With whylogs users can profile massive amounts of data faster than ever before. In our tests, logging 1.2 million rows took 4 seconds and a dataset of 288 million rows x 43 columns took 3.5 minutes! Stay tuned for the upcoming article showing the complete benchmark!

### What integrations do you support?

To check which integrations whylogs support, please see the examples in our [integrations section](https://github.com/whylabs/whylogs/tree/mainline/python/examples/integrations). We are currently migrating integration examples from v0 to v1. If there's an example or integration that you need that you don't see here, please reach out to us on the Rsqrd AI Community [Slack workspace](http://www.bit.ly/rsqrd-slack) in the #whylogs-java-support or #whylogs-python-support channels.


## Privacy

### Does whylogs collect my raw data?

Data is only collected at the time of importing the whylogs library or its modules. We do not collect any sensitive information, raw data or user code. For more information, please see the page [Usage Statistics](usage_statistics.rst)

### What data is collected in whylogs?

whylogs by default collects anonymous information about a user’s environment. This includes information such as: whylogs version, Operating system, Python version and Execution environment. For more information, please see the page [Usage Statistics](usage_statistics.rst)

### How do I disable usage statistics?

To know how to disable usage statistics, please see the page [Usage Statistics](usage_statistics.rst)

## Support

### How do I report a bug I found?

You can report a bug by opening an issue on our [Github page](https://github.com/whylabs/whylogs/issues), or by reaching out to us on our [public Slack channel](https://communityinviter.com/apps/whylabs-community/rsqrd-ai-community), on the #whylogs-python-support channel.
