"""
This is here to verify that the produced wheel includes
all the necessary dependencies. This is exercised by
the CI workflow and does not use pytest because it is
intended to test the wheel in a production environment,
not a development environment.
"""

import os

import pandas as pd

import whylogs
from whylogs import get_or_create_session
from whylogs.app import Session
from whylogs.app.writers import WhyLabsWriter

writer = WhyLabsWriter()
session = Session(writers=[writer])

whylogs_session = get_or_create_session()

print(session)
print(whylogs_session)
print("Successfully created whylogs session with version:")
print(whylogs.__version__)
