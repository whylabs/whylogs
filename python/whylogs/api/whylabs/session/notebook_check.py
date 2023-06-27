def is_notebook() -> bool:
    result = False
    try:
        result = is_ipython_notebook()
    except Exception:
        pass
    return result


def is_ipython_notebook() -> bool:
    """
    Detects whether the current environment is an IPython notebook or not.
    """
    import sys

    return "ipykernel" in sys.modules
