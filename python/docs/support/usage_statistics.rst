Usage Statistics
================

whylogs by default collects anonymous information about a user’s environment. This includes information such as:

whylogs version
Operating system
Python version
Execution environment (Sagemaker, Google Colab, Jupyter Notebook, etc.)
Relevant libraries in the runtime environment (numpy, pandas, etc.)
This data helps our developers to deliver the best possible product to our users. This information can be used to help inform our team of the best areas to focus development and better understand how our users are utilize whylogs.

Data is only collected at the time of importing the whylogs library or its modules. We do not collect any sensitive information, or user code.

If users wish to opt out of this usage statistics collection, they can do so by setting the :code:`WHYLOGS_NO_ANALYTICS` environment variable as follows:

.. code-block:: python

  import os
  os.environ['WHYLOGS_NO_ANALYTICS']='True'

If you have any questions, feel free to reach out to us on the Rsqrd AI Community `Slack workspace <http://www.bit.ly/rsqrd-slack>`_ in the :code:`#whylogs-java-support` or :code:`#whylogs-python-support` channels.
