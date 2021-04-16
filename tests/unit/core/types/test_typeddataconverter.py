from whylogs.core.types import TypedDataConverter


def test_invalid_yaml_returns_string():
    x = " \tSr highway safety Specialist"
    assert x == TypedDataConverter.convert(x)

    # Just verify that `x` is invalid yaml
    import yaml

    try:
        yaml.safe_load(x)
        raise RuntimeError("Should raise exception")
    except yaml.scanner.ScannerError:
        pass
