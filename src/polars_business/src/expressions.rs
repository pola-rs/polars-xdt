use polars::prelude::arity::try_binary_elementwise;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Date)]
fn advance_n_days(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i32()?;
    let n_series = inputs[1].cast(&DataType::Int32)?;
    let n = n_series.i32()?;

    let out = match n.len() {
        1 => {
            if let Some(n) = n.get(0) {
                ca.try_apply(
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
                )
            } else {
                Ok(Int32Chunked::full_null(ca.name(), ca.len()))
            }
        }
        _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
            (Some(s), Some(n)) => {
                let weekday = (s - 4) % 7;

                if weekday == 5 {
                    polars_bail!(ComputeError: "Saturday is not a business date, cannot advance. `roll` argument coming soon.")
                } else if weekday == 6 {
                    polars_bail!(ComputeError: "Sunday is not a business date, cannot advance. `roll` argument coming soon.")
                }

                let n_days = if n >= 0 {
                    n + (n + weekday) / 5 * 2
                } else {
                    -(-n + (-n + 4 - weekday) / 5 * 2)
                };
                Ok(Some(s + n_days))
            }
            _ => Ok(None),
        }),
    };

    out?.cast(&DataType::Date)
}
