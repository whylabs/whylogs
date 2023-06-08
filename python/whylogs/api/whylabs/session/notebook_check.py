def is_notebook() -> bool:
    result = False
    try:
        result = is_ipython_notebook() or is_colab_notebook()
    except Exception:
        pass
    return result


def is_ipython_notebook() -> bool:
    """
    Detects whether the current environment is an IPython notebook or not.
    """
    import sys

    return 'ipykernel' in sys.modules


def is_colab_notebook() -> bool:
    import importlib.util

    return importlib.util.find_spec("google.colab") is not None
