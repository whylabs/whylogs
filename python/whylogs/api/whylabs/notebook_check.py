def is_notebook() -> bool:
    return is_jupyter_notebook() or is_colab_notebook()


def is_jupyter_notebook() -> bool:
    """
    Detects whether the current environment is a Jupyter notebook or not.
    """
    try:
        shell = get_ipython().__class__.__name__  # type: ignore
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False
    except NameError:
        return False  # Probably standard Python interpreter


def is_colab_notebook() -> bool:
    try:
        import google.colab  # type: ignore
        google.colab
        return True
    except NameError:
        return False
