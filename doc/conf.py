import datetime

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "OversightML Model Runner"
copyright = "{}, Amazon.com".format(datetime.datetime.now().year)
author = "Amazon Web Services"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "autoapi.extension",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx_rtd_theme",
]
autoapi_type = "python"
autoapi_dirs = ["../src"]
autoapi_add_toctree_entry = True
autoapi_keep_files = True
autoapi_root = "autoapi"
autoapi_include_summaries = True
autoapi_python_use_implicit_namespaces = True
autoapi_python_class_content = "class"
autoapi_python_extra_arguments = {"members": True, "undoc-members": True, "show-inheritance": True}
autoapi_ignore = ["*/test*", "*/tests/*", "*/__pycache__/*"]
autoapi_member_order = "bysource"
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
    "imported-members",
]

source_suffix = ".rst"
master_doc = "index"

autoclass_content = "class"
autodoc_member_order = "bysource"
default_role = "py:obj"

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# A string that determines how domain objects (e.g. functions, classes,
# attributes, etc.) are displayed in their table of contents entry.
toc_object_entries_show_parents = "hide"

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "logo_only": False,
    "prev_next_buttons_location": "bottom",
    "style_external_links": False,
    "vcs_pageview_mode": "",
    # Toc options
    "collapse_navigation": True,
    "sticky_navigation": True,
    "navigation_depth": 4,
    "includehidden": True,
    "titles_only": False,
}

# For cross-linking to types from other libraries
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Suppress some warnings
suppress_warnings = [
    "ref.python",  # Suppress cross-reference warnings
    "toc.not_included",  # Suppress toctree warnings
]
