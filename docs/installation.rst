Installation
============

First, you need to `install Polars <https://pola-rs.github.io/polars/user-guide/installation/>`_.

Then, you'll need to install `polars-tsx`:

.. code-block::

    pip install polars-tsx

Then, if you can run

.. code-block::

    from datetime import date
    import polars_tsx  # noqa: F401

    print(pts.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True))

it means installation all worked correctly!
