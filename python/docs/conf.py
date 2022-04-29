import os
import sys

sys.path.insert(0, os.path.abspath('../whylogs'))

version = "1.0.0"

project = "whylogs"
author = "whylogs developers"
copyright = "2022, whylogs developers"


extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.githubpages",
    # "numpydoc",
    "sphinx_copybutton",
]

templates_path = ["_templates"]

exclude_patterns = ["_build", "Thumbs.db", "DS_Store"]


html_theme = "furo"
html_title = f"<div class='hidden'>whylogs</div> <div class='version'> v{version}</div>"
html_static_path = ["_static"]
