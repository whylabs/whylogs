# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

import re
import sys

from pathlib import Path

path = Path(__file__)
src_path = path.parents[1].joinpath("whylogs")

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
sys.path.insert(0, src_path.__str__())

from whylogs import __version__

version = re.search(r"([\d.]+)", __version__).group(1)  # type: ignore

# -- Project information -----------------------------------------------------

project = "whylogs"
author = "whylogs developers"
copyright = "2022, whylogs developers"

# -- General configuration ---------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = "1.0"

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named "sphinx.ext.*") or your custom
# ones.
extensions = [
    # "autoapi.extension",
    "sphinx.ext.autodoc",
    "sphinx.ext.githubpages",
    "numpydoc",
    # "nbsphinx",
    "sphinx_copybutton",
    "sphinx_autodoc_typehints",
]

# Don't execute the notebook cells when generating the documentation
# This can be configured on a per notebook basis as well
# See: https://nbsphinx.readthedocs.io/en/0.2.15/never-execute.html#Explicitly-Dis-/
nbsphinx_execute = "never"
nbsphinx_prolog = """
|

|github_link| or |binder_link|!

----

.. |github_link| raw:: html

   <a href="https://github.com/capitalone/rubicon-ml/tree/main/notebooks/{{ env.doc2path(env.docname, base=None) }}"
   target="_blank">View this notebook on GitHub</a>

.. |binder_link| raw:: html

   <a href="https://mybinder.org/v2/gh/capitalone/rubicon-ml/main?filepath=notebooks/{{ env.doc2path(env.docname,
   base=None) }}" target="_blank">run it yourself on Binder</a>
"""

autodoc_default_flags = ["members", "inherited-members"]

numpydoc_class_members_toctree = False
numpydoc_show_class_members = False
numpydoc_attributes_as_param_list = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = [".rst", ".md"]
source_suffix = ".rst"

add_module_names = False

# The master toctree document.
master_doc = "index"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.
html_theme = "furo"
html_title = f"<div class='hidden'>whylogs</div> <div class='version'> v{version}</div>"
html_static_path = ["_static"]
