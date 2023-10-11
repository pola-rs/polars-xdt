use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Date)]
fn advance_n_days(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i32()?;
    let n = inputs[1].i32()?.get(0).unwrap();

    let out = ca.try_apply(
        |x|{
        let weekday = (x - 4) % 7;

        if weekday == 5 {
            polars_bail!(ComputeError: "Saturday is not a business date, cannot advance. `roll` argument coming soon.")
        } else if weekday == 6 {
            polars_bail!(ComputeError: "Sunday is not a business date, cannot advance. `roll` argument coming soon.")
        }

        let n_days = if n >= 0 {
            n + (n + weekday) / 5 * 2
        } else {
            -(-n + (-n + 4-weekday) / 5 * 2)
        };
        Ok(x + n_days)
        }
    )?;
    Ok(out.cast(&DataType::Date).unwrap().into_series())
}
