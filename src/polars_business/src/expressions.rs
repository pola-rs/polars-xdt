use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Date)]
fn advance_n_days(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i32()?;
    let n = inputs[1].i32()?.get(0).unwrap();

    let out = ca.apply_values(
        |mut x|{
        let mut weekday = (x - 4) % 7;

        // If on weekend, roll backwards to previous
        // valid date (following pandas here).
        if weekday == 5 {
            x -= 1;
            weekday = 4;
        } else if weekday == 6 {
            x -= 2;
            weekday = 4;
        }

        let n_days = n + (n + weekday) / 5 * 2;
        x + n_days
        }
    );
    Ok(out.cast(&DataType::Date).unwrap().into_series())
}
