#!/usr/bin/env python3
"""
A script to generate some example data
"""
if __name__ == "__main__":
    import os

    from whylabs.logs.core import DatasetProfile
    from whylabs.logs.util.protobuf import message_to_json

    MYDIR = os.path.realpath(os.path.dirname(__file__))
    d = os.getcwd()
    try:
        os.chdir(MYDIR)
        vals = [1, 2, 1.0, 2.0, "!12#&", "blah", True, False] * 100
        prof = DatasetProfile("some_dataset")
        for v in vals:
            prof.track("some_column", v)
        with open("column_summary.json", "wt") as fp:
            print("writing:", "column_summary.json")
            fp.write(message_to_json(prof.columns["some_column"].to_summary()))
        with open("dataset_summary.json", "wt") as fp:
            print("writing:", "dataset_summary.json")
            fp.write(message_to_json(prof.to_summary()))
    finally:
        os.chdir(d)
