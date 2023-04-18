def is_notebook() -> bool:
    try:
        get_ipython()  # type: ignore
        return True
    except NameError:
        return False
