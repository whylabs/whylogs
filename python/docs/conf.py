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
    'sphinx.ext.autosectionlabel',
    # "numpydoc",
    "sphinx_copybutton",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "myst_parser",
]

autosectionlabel_prefix_document = True

pygments_style = "friendly"
pygments_dark_style = "material"

templates_path = ["_templates"]

exclude_patterns = ["_build", "Thumbs.db", "DS_Store"]

index_doc = "index"

html_theme = "furo"
html_title = f"<div class='hidden'>whylogs</div> <div class='version'> v{version}</div>"
html_static_path = ["_static"]

intersphinx_mapping = {
    'py': ('https://docs.python.org/{0.major}.{0.minor}'.format(sys.version_info), None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
}

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}

myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]
