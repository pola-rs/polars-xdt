use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Date)]
fn add_bday(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].date()?;
    Ok(ca.clone().into_series())
}

