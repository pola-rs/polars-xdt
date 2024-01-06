Installation
============

First, you need to `install Polars <https://pola-rs.github.io/polars/user-guide/installation/>`_.

Then, you'll need to install `polars-xdt`:

.. code-block::

    pip install polars-xdt

Then, if you can run

.. code-block::

    import polars as pl
    import polars_xdt  # noqa: F401

    print(pl.col('a').xdt)

and see something like `<polars_xdt.ExprXDTNamespace at 0x7f5bc943fc10>`,
it means installation all worked correctly!
