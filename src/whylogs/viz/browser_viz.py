import os
import webbrowser

_MY_DIR = os.path.realpath(os.path.dirname(__file__))


def profile_viewer():
    """
    open a profile viewer loader on your default browser
    """
    index_path = os.path.abspath(os.path.join(_MY_DIR, os.pardir, "viewer", "index.html"))
    return webbrowser.open_new_tab(f"file:{index_path}")
