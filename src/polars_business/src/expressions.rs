use polars::prelude::arity::try_binary_elementwise;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn weekday(x: i32) -> i32 {
    (x - 4) % 7
}

fn count_holidays(start: i32, end: i32, holidays: &mut Vec<Option<i32>>) -> i32 {
    let mut counter = 0;  // Initialize the counter to 0

    // Iterate over the indices of the holidays vector in reverse order
    // so that we can remove elements without causing issues with indexing
    let mut index = holidays.len() as i32 - 1;
    while index >= 0 {
        // Check if the holiday is within the specified range and is Some
        if let Some(value) = holidays[index as usize] {
            if value >= start && value <= end {
                // If the holiday is within the range, increment the counter
                counter += 1;
                // Remove the holiday from the vector
                holidays.remove(index as usize);
            }
        }
        // Move to the previous index
        index -= 1;
    }

    // Return the final counter
    counter
}

#[polars_expr(output_type=Date)]
fn advance_n_days(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i32()?;
    let n_series = inputs[1].cast(&DataType::Int32)?;
    let n = n_series.i32()?;

    let vec = if inputs.len() == 3 {
        let binding = inputs[2].list()?.get(0).unwrap();
        let holidays = binding.i32()?;
        Vec::from(holidays)
    } else {
        Vec::new()
    };
    let vec: Vec<_> = vec.into_iter().filter(|x| x.map(|x| weekday(x) < 5).unwrap_or(false)).collect();

    let out = match n.len() {
        1 => {
            if let Some(n) = n.get(0) {
                ca.try_apply(
                    |x|{
                        let x_weekday = weekday(x);

                        if x_weekday == 5 {
                            polars_bail!(ComputeError: "Saturday is not a business date, cannot advance. `roll` argument coming soon.")
                        } else if x_weekday == 6 {
                            polars_bail!(ComputeError: "Sunday is not a business date, cannot advance. `roll` argument coming soon.")
                        }

                        let mut n_days = if n >= 0 {
                            n + (n + x_weekday) / 5 * 2
                        } else {
                            -(-n + (-n + 4-x_weekday) / 5 * 2)
                        };
                        if vec.len() > 0 {
                            let mut myvec = vec.clone();
                            let mut count_hols = count_holidays(x, x+n_days, &mut myvec);
                            while count_hols > 0 {
                                for _ in 0..count_hols {
                                    n_days += 1;
                                    let res = x + n_days;
                                    let weekday_res = weekday(res);
                                    if weekday_res == 5 {
                                        n_days += 2;
                                    } else if weekday_res == 6 {
                                        n_days += 1;
                                    };
                                }
                                count_hols = count_holidays(x, x+n_days, &mut myvec);
                            }
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
                let weekday_s = weekday(s);

                if weekday_s == 5 {
                    polars_bail!(ComputeError: "Saturday is not a business date, cannot advance. `roll` argument coming soon.")
                } else if weekday_s == 6 {
                    polars_bail!(ComputeError: "Sunday is not a business date, cannot advance. `roll` argument coming soon.")
                }

                let n_days = if n >= 0 {
                    n + (n + weekday_s) / 5 * 2
                } else {
                    -(-n + (-n + 4 - weekday_s) / 5 * 2)
                };
                Ok(Some(s + n_days))
            }
            _ => Ok(None),
        }),
    };

    out?.cast(&DataType::Date)
}
