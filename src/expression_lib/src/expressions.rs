use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Date)]
fn add_bday(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].date()?;
    let out = ca.apply(|x| x.map(|x| x+1));
    Ok(out.cast(&DataType::Date).unwrap().into_series())
}

