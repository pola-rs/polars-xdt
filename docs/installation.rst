Installation
============

First, you need to `install Polars <https://pola-rs.github.io/polars/user-guide/installation/>`_.

Then, you'll need to install `polars-xdt`:

.. code-block::

    pip install polars-xdt

Then, if you can run

.. code-block::

    from datetime import date
    import polars_xdt  # noqa: F401

    print(xdt.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True))

it means installation all worked correctly!
