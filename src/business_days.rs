use chrono::NaiveDateTime;
use polars::prelude::arity::try_binary_elementwise;
use polars::prelude::*;

pub(crate) fn weekday(x: i32) -> i32 {
    // the first modulo might return a negative number, so we add 7 and take
    // the modulo again so we're sure we have something between 0 and 6
    ((x - 4) % 7 + 7) % 7 + 1
}

pub(crate) fn calculate_advance(
    mut date: i32,
    mut offset: i32,
    mut day_of_week: i32,
    weekmask: &[bool; 7],
    n_weekdays: i32,
    holidays: &[i32],
    roll: &str,
) -> PolarsResult<i32> {
    match roll {
        "raise" => {
            if holidays.contains(&date)
                | unsafe { !*weekmask.get_unchecked(day_of_week as usize - 1) }
            {
                let date = NaiveDateTime::from_timestamp_opt(date as i64 * 24 * 60 * 60, 0)
                    .unwrap()
                    .format("%Y-%m-%d");
                polars_bail!(ComputeError:
                    format!("date {} is not a business date, cannot advance; set a valid `roll` strategy.", date)
                )
            };
        }
        "forward" => {
            while holidays.contains(&date)
                | unsafe { !*weekmask.get_unchecked(day_of_week as usize - 1) }
            {
                date += 1;
                day_of_week += 1;
                if day_of_week > 7 {
                    day_of_week = 1;
                }
            }
        }
        "backward" => {
            while holidays.contains(&date)
                | unsafe { !*weekmask.get_unchecked(day_of_week as usize - 1) }
            {
                date -= 1;
                day_of_week -= 1;
                if day_of_week == 0 {
                    day_of_week = 7;
                }
            }
        }
        _ => {
            polars_bail!(InvalidOperation:
                "`roll` must be one of 'raise', 'forward' or 'backward'; found '{}'", roll
            )
        }
    }

    if offset > 0 {
        let holidays_begin = match holidays.binary_search(&date) {
            Ok(x) => x,
            Err(x) => x,
        };

        date += (offset / n_weekdays) * 7;
        offset %= n_weekdays;

        let holidays_temp = match holidays[holidays_begin..].binary_search(&date) {
            Ok(x) => x + 1,
            Err(x) => x,
        } + holidays_begin;

        offset += (holidays_temp - holidays_begin) as i32;
        let holidays_begin = holidays_temp;

        while offset > 0 {
            date += 1;
            day_of_week += 1;
            if day_of_week > 7 {
                day_of_week = 1;
            }
            if unsafe {
                (*weekmask.get_unchecked(day_of_week as usize - 1))
                    && (!holidays[holidays_begin..].contains(&date))
            } {
                offset -= 1;
            }
        }
        Ok(date)
    } else {
        let holidays_end = match holidays.binary_search(&date) {
            Ok(x) => x + 1,
            Err(x) => x,
        };

        date += (offset / n_weekdays) * 7;
        offset %= n_weekdays;

        let holidays_temp = match holidays[..holidays_end].binary_search(&date) {
            Ok(x) => x,
            Err(x) => x,
        };

        offset -= (holidays_end - holidays_temp) as i32;
        let holidays_end = holidays_temp;

        while offset < 0 {
            date -= 1;
            day_of_week -= 1;
            if day_of_week == 0 {
                day_of_week = 7;
            }
            if unsafe {
                (*weekmask.get_unchecked(day_of_week as usize - 1))
                    && (!holidays[..holidays_end].contains(&date))
            } {
                offset += 1;
            }
        }
        Ok(date)
    }
}

pub(crate) fn impl_advance_n_days(
    s: &Series,
    n: &Series,
    holidays: Vec<i32>,
    weekmask: &[bool; 7],
    roll: &str,
) -> PolarsResult<Series> {
    let original_dtype = s.dtype();

    // Only keep holidays which aren't on weekends.
    let holidays: Vec<i32> = {
        holidays
            .into_iter()
            .filter(|x| unsafe { *weekmask.get_unchecked(weekday(*x) as usize - 1) })
            .collect()
    };

    let n_weekdays = weekmask.iter().filter(|&x| *x).count() as i32;

    let n = n.i32()?;

    match s.dtype() {
        DataType::Date => {
            let ca = s.date()?;
            let out = match n.len() {
                1 => {
                    if let Some(n) = n.get(0) {
                        ca.try_apply(|x_date| {
                            let x_weekday = weekday(x_date);
                            calculate_advance(
                                x_date, n, x_weekday, weekmask, n_weekdays, &holidays, roll,
                            )
                        })
                    } else {
                        Ok(Int32Chunked::full_null(ca.name(), ca.len()))
                    }
                }
                _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
                    (Some(x_date), Some(n)) => {
                        let x_weekday = weekday(x_date);
                        Ok(Some(calculate_advance(
                            x_date, n, x_weekday, weekmask, n_weekdays, &holidays, roll,
                        )?))
                    }
                    _ => Ok(None),
                }),
            };
            out?.cast(original_dtype)
        }
        DataType::Datetime(time_unit, time_zone) => {
            let multiplier = match time_unit {
                TimeUnit::Milliseconds => 60 * 60 * 24 * 1_000,
                TimeUnit::Microseconds => 60 * 60 * 24 * 1_000_000,
                TimeUnit::Nanoseconds => 60 * 60 * 24 * 1_000_000_000,
            };
            let ca = &polars_ops::prelude::replace_time_zone(
                s.datetime()?,
                None,
                &StringChunked::from_iter(std::iter::once("raise")),
            )?;
            let out = match n.len() {
                1 => {
                    if let Some(n) = n.get(0) {
                        ca.try_apply(|x| {
                            let x_date = (x / multiplier) as i32;
                            let x_weekday = weekday(x_date);
                            Ok(x + ((calculate_advance(
                                x_date, n, x_weekday, weekmask, n_weekdays, &holidays, roll,
                            )? - x_date) as i64
                                * multiplier))
                        })
                    } else {
                        Ok(Int64Chunked::full_null(ca.name(), ca.len()))
                    }
                }
                _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
                    (Some(x), Some(n)) => {
                        let x_date = (x / multiplier) as i32;
                        let x_weekday = weekday(x_date);
                        Ok(Some(
                            x + ((calculate_advance(
                                x_date, n, x_weekday, weekmask, n_weekdays, &holidays, roll,
                            )? - x_date) as i64
                                * multiplier),
                        ))
                    }
                    _ => Ok(None),
                }),
            };
            let out = polars_ops::prelude::replace_time_zone(
                &out?.into_datetime(*time_unit, None),
                time_zone.as_deref(),
                &StringChunked::from_iter(std::iter::once("raise")),
            )?;
            out.cast(original_dtype)
        }
        _ => {
            polars_bail!(ComputeError: format!("expected Datetime or Date dtype, got: {}", original_dtype))
        }
    }
}
