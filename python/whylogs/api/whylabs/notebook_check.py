import os


def is_notebook() -> bool:
    try:
        get_ipython()  # type: ignore
        return True
    except NameError:
        return False


def is_git_actions() -> bool:
    git_actions_var = os.getenv("GITHUB_ACTIONS")
    if git_actions_var:
        return True
    return False
