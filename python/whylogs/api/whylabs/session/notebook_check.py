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
    if "get_ipython" in dir():
        import importlib.util

        if importlib.util.find_spec("IPython"):
            from IPython import get_ipython

            return get_ipython().__class__.__name__ == "ZMQInteractiveShell"

    return False


def is_colab_notebook() -> bool:
    import importlib.util

    return importlib.util.find_spec("google.colab") is not None
