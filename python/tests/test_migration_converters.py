import os

from whylogs.migration.converters import read_v0_to_view

script_dir = os.path.dirname(os.path.realpath(__file__))


def test_convert_v0_to_v1_view() -> None:
    v1_view = read_v0_to_view(f"{script_dir}/v0_profile.bin")
    assert len(v1_view.to_pandas()) == 22
