use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Date)]
fn advance_by_days(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i32()?;
    let n = inputs[1].i32()?.get(0).unwrap();

    let out = ca.apply_values(
        |x|{
        let w = (x / (3600 * 24 - 3)) % 7 + 1;
        let n_days = n + (n + w) / 5 * 2;
        x + n_days
        }
    );
    Ok(out.cast(&DataType::Date).unwrap().into_series())
}
