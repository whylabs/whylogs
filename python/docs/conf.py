import os
import sys
import shutil
from typing import Any, Dict

if shutil.which("pandoc") is None:
    print("PLEASE INSTALL PANDOC <https://pandoc.org/>")
    print("Pandoc is required to build our documentation.")
    sys.exit(1)

version = "1.6.1"

project = "whylogs"
author = "whylogs developers"
copyright = "2022, whylogs developers"

extensions = [
    "sphinx.ext.napoleon",
    # Autoapi
    "autoapi.extension",
    "sphinx.ext.autodoc",
    "sphinx.ext.autodoc.typehints",
    "nbsphinx",
    "sphinx.ext.githubpages",
    "sphinx.ext.autosectionlabel",
    "sphinxext.opengraph",
    "sphinx_inline_tabs",
    "sphinx_copybutton",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "myst_parser",
]

autosectionlabel_prefix_document = True

autoapi_python_use_implicit_namespaces = True
autosummary_generate = True
autoapi_keep_files = True
autoapi_root = "api"
autodoc_typehints = "both"
autoapi_dirs = [os.path.abspath("../whylogs")]
autoapi_options = [
    "members",
    "inherited-members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "imported-members",
]
autoapi_ignore = ["*utils*", "*stubs*"]
always_document_param_types = True
typehints_defaults = "braces"

pygments_style = "friendly"
pygments_dark_style = "material"

templates_path = ["_templates"]

exclude_patterns = ["_build", "Thumbs.db", "DS_Store"]

index_doc = "index"
master_doc = "index"

html_theme = "furo"
html_title = f"<div class='hidden'>whylogs</div>  <div class='version'> v{version}</div>"
html_static_path = ["_static"]
html_css_files = ["custom.css"]

# do not make images clickable. It gets quite annoying otherwise
# see: https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-html_scaled_image_link
html_scaled_image_link = False

intersphinx_mapping = {
    "py": ("https://docs.python.org/{0.major}.{0.minor}".format(sys.version_info), None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}

# opengraph
ogp_site_url = "https://whylogs.readthedocs.io"

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

html_theme_options: Dict[str, Any] = {
    "announcement": """<em>whylogs v1</em> has been launched! Make sure you checkout the
    <a href="/en/latest/migration/basics.html" alt="Link to migration guide">the migration guide</a> to ensure a smooth
    transition""",
    "light_logo": "images/logo.png",
    "dark_logo": "images/logo.png",
    "footer_icons": [
        {
            "name": "Slack",
            "url": "https://bit.ly/whylogs-slack",
            "html": """
            <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 512 512"><path d="M213.6
            236.216l64.003-21.438 20.708 61.823-64.004
            21.438z"></path><path d="M213.6 236.216l64.003-21.438 20.708 61.823-64.004 21.438z"></path><path
            d="M475.9 190C426.4 25 355-13.4 190 36.1S-13.4 157 36.1 322 157 525.4 322 475.9 525.4 355 475.9
            190zm-83.3 107.1l-31.1 10.4 10.7 32.2c4.2 13-2.7 27.2-15.7 31.5-2.7.8-5.8 1.5-8.4
            1.2-10-.4-19.6-6.9-23-16.9l-10.7-32.2-64.1 21.5L261 377c4.2 13-2.7 27.2-15.7 31.5-2.7.8-5.8 1.5-8.4
            1.2-10-.4-19.6-6.9-23-16.9L203 360.4l-31 10.3c-2.7.8-5.8 1.5-8.4 1.2-10-.4-19.6-6.9-23-16.9-4.2-13
            2.7-27.2 15.7-31.5l31.1-10.4-20.7-61.8-31.1 10.4c-2.7.8-5.8 1.5-8.4 1.2-10-.4-19.6-6.9-23-16.9-4.2-13
            2.7-27.2 15.7-31.5l31.1-10.4-10.9-32.1c-4.2-13 2.7-27.2 15.7-31.5 13-4.2 27.2 2.7 31.5 15.7l10.7 32.2
            64.1-21.5-10.7-32.2c-4.2-13 2.7-27.2 15.7-31.5 13-4.2 27.2 2.7 31.5 15.7l10.7 32.2 31.1-10.4c13-4.2 27.2
            2.7 31.5 15.7 4.2 13-2.7 27.2-15.7 31.5l-31.1 10.4 20.7 61.8 31.1-10.4c13-4.2 27.2 2.7 31.5 15.7 4.2
            13.2-2.7 27.4-15.8 31.7z"></path></svg>
            """,
            "class": "",
        },
        {
            "name": "GitHub",
            "url": "https://github.com/whylabs/whylogs",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16">
                    <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38
                    0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53
                    .63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95
                    0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0
                    1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87
                    3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16
                    8c0-4.42-3.58-8-8-8z"></path>
                </svg>
            """,
            "class": "",
        },
    ],
}

# Notebooks
nbsphinx_execute = "never"
