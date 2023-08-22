def is_ipython_notebook() -> bool:
    """
    Detects whether the current environment is an IPython notebook or not.
    """
    import sys

    return "ipykernel" in sys.modules


def is_ipython_terminal() -> bool:
    # Works for colab too
    try:
        __IPYTHON__  # type: ignore
        return True
    except NameError:
        return False


def is_interractive() -> bool:
    return is_ipython_terminal() or is_ipython_notebook()
