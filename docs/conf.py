# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))
import os
import re
import sys
import uuid
from collections import namedtuple

import sphinx_rtd_theme
from sphinx.ext.autodoc import between

# Add package paths to the python path to allow sphinx to import modules
sys.path.insert(0, os.path.abspath("../src/"))

# -- Project information -----------------------------------------------------

project = "whylogs"
copyright = "2020, WhyLabs, Inc"
author = "WhyLabs"
gitstamp_fmt = "%d %b %Y"


def read(rel_path):
    import codecs

    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


# This string is managed by bump2version
version = " 0.1.5b0"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx_gallery.gen_gallery",
    "autoapi.extension",
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    # 'sphinx.ext.mathjax'
    "sphinx.ext.napoleon",
    "sphinxcontrib.contentui",
    "sphinx_gitstamp",
    "sphinx.ext.autosectionlabel",
    "sphinx_autorun",
]

sphinx_gallery_conf = {
    "examples_dirs": "../examples",  # path to your example scripts
    "gallery_dirs": "auto_examples",  # path to where to save gallery generated output
    # 'filename_pattern': '/plot_',  # Default pattern of filenames to execute
    # 'ignore_pattern': r'__init__\.py',  # Default filename ignore pattern
    # 'filename_pattern': '.*\.py$',
    # 'ignore_pattern': '.*',
}


autoapi_type = "python"
autoapi_dirs = ["../src/whylogs"]
autoapi_options = [
    "members",
    "undoc-members",
    "private-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
    "imported-members",
    "inherited-members",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

index_doc = "index"


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["style.css"]

html_theme_options = {
    "navigation_depth": 6,
    # 'github_url': 'https://github.com/whylabs/whylogs-python',
}
