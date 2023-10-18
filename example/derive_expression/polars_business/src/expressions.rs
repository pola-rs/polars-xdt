use polars::prelude::arity::try_binary_elementwise;
use ahash::AHashMap;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

fn weekday(x: i32) -> i32 {
    // the first modulo might return a negative number, so we add 7 and take
    // the modulo again so we're sure we have something between 0 and 6
    ((x - 4) % 7 + 7) % 7
}

fn increment(x: i32) -> i32 {
    if x > 0 {
        x + 1
    } else {
        x - 1
    }
}

fn advance_few_days(x_weekday: i32, n: i32, weekend: &[i32]) -> i32 {
    // We know that n is between -7 and 7
    // and that x_weekday is between 0 and 6
    // could we pre-compute all the possible values?
    let mut n_days = 0;
    for _ in 0..n.abs() {
        if n > 0 {
            n_days += 1;
            n_days = roll(n_days, (x_weekday + n_days)%7, weekend);
        } else {
            n_days -= 1;
            n_days = roll(n_days, (x_weekday+n_days+7)%7, weekend);
        }
    }
    n_days
}

fn calculate_n_days_without_holidays_slow(x_weekday: i32, n: i32, n_weekend: i32, cache: &AHashMap<i32, i32>) -> i32 {
    let n_weeks = n / (7-n_weekend);
    let n_days = n % (7-n_weekend);
    let n_days = cache.get(&(n_days*10 + x_weekday)).unwrap();
    n_days + n_weeks * 7
}

fn calculate_n_days_without_holidays_blazingly_fast(_x: i32, n: i32, _x_weekday: i32, _weekend: &[i32], _n_weekend: i32) -> i32 {
    n
}

fn calculate_n_days_without_holidays_fast(n: i32, x_weekday: i32) -> i32 {
    if n >= 0 {
        n + (n + x_weekday) / 5 * 2
    } else {
        -(-n + (-n + 4 - x_weekday) / 5 * 2)
    }
}

fn roll(n_days: i32, x_weekday: i32, weekend: &[i32]) -> i32 {
    let mut x_weekday = x_weekday;
    let mut n_days = n_days;
    while weekend.contains(&x_weekday) {
        if n_days > 0 {
            x_weekday = (x_weekday + 1) % 7;
        } else {
            x_weekday = (x_weekday - 1 + 7) % 7;
        }
        n_days = increment(n_days);
    }
    n_days
}

fn calculate_n_days_with_holidays(x: i32, n: i32, holidays: &[i32]) -> PolarsResult<i32> {
    let x_mod_7 = x % 7;
    let x_weekday = weekday(x_mod_7);

    if x_weekday >= 5 {
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    };

    let mut n_days = calculate_n_days_without_holidays_fast(n, x_weekday);

    if holidays.binary_search(&x).is_ok() {
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    }
    let mut count_hols = count_holidays(x, x + n_days, &holidays);
    while count_hols > 0 {
        let n_days_before = n_days;
        if n_days > 0 {
            n_days = n_days + calculate_n_days_without_holidays_fast(count_hols, weekday(x_mod_7 + n_days));
            count_hols = count_holidays(x+n_days_before+1, x + n_days, &holidays);
        } else {
            n_days = n_days + calculate_n_days_without_holidays_fast(-count_hols, weekday(x_mod_7 + n_days));
            count_hols = count_holidays(x+n_days_before-1, x + n_days, &holidays);
        }
    }
    Ok(n_days)
}

fn calculate_n_days_with_weekend_and_holidays(x: i32, n: i32, weekend: &[i32], cache: &AHashMap<i32, i32>, holidays: &[i32]) -> PolarsResult<i32> {
    let x_mod_7 = x % 7;
    let x_weekday = weekday(x_mod_7);

    if weekend.contains(&x_weekday){
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    };

    let mut n_days = calculate_n_days_without_holidays_slow(x_weekday, n, weekend.len() as i32, cache);

    if holidays.binary_search(&x).is_ok() {
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    }
    let mut count_hols = count_holidays(x, x + n_days, &holidays);
    while count_hols > 0 {
        let n_days_before = n_days;
        if n_days > 0 {
            n_days = n_days + calculate_n_days_without_holidays_slow(weekday(x_mod_7+n_days), count_hols, weekend.len() as i32, cache);
            count_hols = count_holidays(x+n_days_before+1, x + n_days, &holidays);
        } else {
            n_days = n_days + calculate_n_days_without_holidays_slow(weekday(x_mod_7+n_days), -count_hols, weekend.len() as i32, cache);
            count_hols = count_holidays(x+n_days_before-1, x + n_days, &holidays);
        }
    }
    Ok(n_days)
}

fn calculate_n_days_with_weekend(x: i32, n: i32, weekend: &[i32], cache: &AHashMap<i32, i32>) -> PolarsResult<i32> {
    let x_mod_7 = x % 7;
    let x_weekday = weekday(x_mod_7);
    let n_weekend = weekend.len() as i32;

    if weekend.contains(&x_weekday){
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    };

    Ok(calculate_n_days_without_holidays_slow(x_weekday, n, n_weekend, cache))
}


fn count_holidays(start: i32, end: i32, holidays: &[i32]) -> i32 {
    if end >= start {
        let start_pos = match holidays.binary_search(&start) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let end_pos = match holidays.binary_search(&end) {
            Ok(pos) => pos+1,
            Err(pos) => pos,
        };
        end_pos as i32 - start_pos as i32
    } else {
        let start_pos = match holidays.binary_search(&end) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let end_pos = match holidays.binary_search(&start) {
            Ok(pos) => pos+1,
            Err(pos) => pos,
        };
        end_pos as i32 - start_pos as i32
    }
}

#[derive(Deserialize)]
pub struct BusinessDayKwargs {
    holidays: Vec<i32>,
}

fn bday_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = input_fields[0].clone();
    Ok(field)
}

#[polars_expr(type_func=bday_output)]
fn advance_n_days(
    inputs: &[Series],
    kwargs: BusinessDayKwargs,
) -> PolarsResult<Series> {
    let n_series = inputs[1].cast(&DataType::Int32)?;
    let n = n_series.i32()?;

    let holidays = kwargs.holidays;
    let mut holidays: Vec<_> = holidays
        .into_iter()
        .filter(|x| weekday(*x) < 5)
        .collect();
    holidays.sort();

    let s= &inputs[0];
    let original_dtype = s.dtype();
    match s.dtype() {
        DataType::Date => {
            let ca = inputs[0].date()?;
            let out = match n.len() {
                1 => {
                    if let Some(n) = n.get(0) {
                        if holidays.is_empty() {
                            ca.try_apply(|x_date| {
                                let x_weekday = weekday(x_date);
                                if x_weekday >= 5 {
                                    polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x_date))
                                }
                                Ok(x_date+(calculate_n_days_without_holidays_fast(n, x_weekday)))
                            })
                        } else {
                            ca.try_apply(|x_date| Ok(x_date + calculate_n_days_with_holidays(x_date, n, &holidays)?))
                        }
                    } else {
                        Ok(Int32Chunked::full_null(ca.name(), ca.len()))
                    }
                }
                _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
                    (Some(x_date), Some(n)) => {
                        let x_weekday = weekday(x_date);
                        if x_weekday >= 5 {
                            polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x_date))
                        }
                        if holidays.is_empty() {
                            Ok(Some(x_date+calculate_n_days_without_holidays_fast(n, x_weekday)))
                        } else {
                            Ok(x_date + calculate_n_days_with_holidays(x_date, n, &holidays)?).map(Some)
                        }
                    }
                    _ => Ok(None),
                }),
            };
            out?.cast(original_dtype)
        }
        DataType::Datetime(time_unit, time_zone) => {
            let multiplier = match time_unit {
                TimeUnit::Milliseconds => 60*60*24*1_000,
                TimeUnit::Microseconds => 60*60*24*1_000_000,
                TimeUnit::Nanoseconds => 60*60*24*1_000_000_000,
            };
            let ca = &polars_ops::prelude::replace_time_zone(s.datetime()?, None, &Utf8Chunked::from_iter(std::iter::once("raise")))?;
            let out = match n.len() {
                1 => {
                    if let Some(n) = n.get(0) {
                        ca.try_apply(|x| {
                            let x_date = (x / multiplier) as i32;
                            let x_weekday = weekday(x_date);
                            if x_weekday >= 5 {
                                polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x_date))
                            }
                            Ok(x+(calculate_n_days_without_holidays_fast(n, x_weekday) as i64 *multiplier))
                        })
                    } else {
                        Ok(Int64Chunked::full_null(ca.name(), ca.len()))
                    }
                }
                _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
                    (Some(x), Some(n)) => {
                        let x_date = (x / multiplier) as i32;
                        let x_weekday = weekday(x_date);
                        if x_weekday >= 5 {
                            polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
                        }
                        Ok(Some(x+(calculate_n_days_without_holidays_fast(n, x_weekday) as i64 *multiplier)))
                    }
                    _ => Ok(None),
                }),
            };
            let out = polars_ops::prelude::replace_time_zone(&out?.into_datetime(*time_unit, None), time_zone.as_deref(), &Utf8Chunked::from_iter(std::iter::once("raise")))?;
            out.cast(original_dtype)
        },
        _ => polars_bail!(ComputeError: format!("expected Datetime or Date dtype, got: {}", original_dtype)),
    }
}
