# ruff: noqa
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import sphinx_autosummary_accessors  # noqa: F401

project = "polars-xdt"
copyright = "2023, Marco Gorelli"
author = "Marco Gorelli"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    "numpydoc",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_favicon",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
numpydoc_show_class_members = False


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
# html_static_path = ["_static"]

intersphinx_mapping = {
    "polars": ("https://pola-rs.github.io/polars/py-polars/html/", None),
}

copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True

html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "navigation_with_keys": False,
}

numpydoc_show_class_members = False
