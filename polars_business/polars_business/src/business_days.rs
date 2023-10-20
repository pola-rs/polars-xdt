use ahash::AHashMap;
use polars::prelude::arity::try_binary_elementwise;
use polars::prelude::*;
use chrono::NaiveDateTime;

pub(crate) fn weekday(x: i32) -> i32 {
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

pub(crate) fn advance_few_days(x_weekday: i32, n: i32, weekend: &[i32]) -> i32 {
    // We know that n is between -7 and 7
    // and that x_weekday is between 0 and 6
    // could we pre-compute all the possible values?
    let mut n_days = 0;
    for _ in 0..n.abs() {
        if n > 0 {
            n_days += 1;
            n_days = roll(n_days, (x_weekday + n_days) % 7, weekend);
        } else {
            n_days -= 1;
            n_days = roll(n_days, (x_weekday + n_days + 7) % 7, weekend);
        }
    }
    n_days
}

pub(crate) fn calculate_n_days_without_holidays_slow(
    x_weekday: i32,
    n: i32,
    n_weekdays: i32,
    cache: &AHashMap<i32, i32>,
) -> i32 {
    let (n_weeks, n_days) = (n / n_weekdays, n % n_weekdays);
    let n_days = cache.get(&(n_days * 10 + x_weekday)).unwrap();
    n_days + n_weeks * 7
}

fn calculate_n_days_without_holidays_blazingly_fast(n: i32, x_weekday: i32) -> i32{
    if n >= 0 {
        n + (n + x_weekday) / 5 * 2
    } else {
        -(-n + (-n + 4 - x_weekday) / 5 * 2)
    }
}

fn calculate_n_days_without_holidays_fast(
    x_date: i32,
    _x_mod_7: i32,
    n: i32,
    x_weekday: i32,
    _weekend: &[i32],
    _cache: Option<&AHashMap<i32, i32>>,
    _holidays: &[i32],
) -> PolarsResult<i32> {
    if x_weekday >= 5 {
        return its_a_business_date_error_message(x_date);
    }
    Ok(calculate_n_days_without_holidays_blazingly_fast(n, x_weekday))
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

fn its_a_business_date_error_message(x: i32) -> PolarsResult<i32>{
    let date = NaiveDateTime::from_timestamp_opt(x as i64 * 24 * 60 * 60, 0).unwrap().format("%Y-%m-%d");
    polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", date))
}

pub(crate) fn calculate_n_days_with_holidays(
    x_date: i32,
    x_mod_7: i32,
    n: i32,
    x_weekday: i32,
    _weekend: &[i32],
    _cache: Option<&AHashMap<i32, i32>>,
    holidays: &[i32],
) -> PolarsResult<i32> {
    if x_weekday >= 5 {
        return its_a_business_date_error_message(x_date);
    };

    let mut n_days = calculate_n_days_without_holidays_blazingly_fast(n, x_weekday);

    if holidays.binary_search(&x_date).is_ok() {
        return its_a_business_date_error_message(x_date);
    }
    let mut count_hols = count_holidays(x_date, x_date + n_days, holidays);
    while count_hols > 0 {
        let n_days_before = n_days;
        if n_days > 0 {
            n_days = n_days
                + calculate_n_days_without_holidays_blazingly_fast(count_hols, weekday(x_mod_7 + n_days));
            count_hols = count_holidays(x_date + n_days_before + 1, x_date + n_days, holidays);
        } else {
            n_days = n_days
                + calculate_n_days_without_holidays_blazingly_fast(-count_hols, weekday(x_mod_7 + n_days));
            count_hols = count_holidays(x_date + n_days_before - 1, x_date + n_days, holidays);
        }
    }
    Ok(n_days)
}

pub(crate) fn calculate_n_days_with_weekend_and_holidays(
    x: i32,
    x_mod_7: i32,
    n: i32,
    x_weekday: i32,
    weekend: &[i32],
    cache: Option<&AHashMap<i32, i32>>,
    holidays: &[i32],
) -> PolarsResult<i32> {
    let cache = cache.unwrap();
    let n_weekdays = 7 - weekend.len() as i32;

    if weekend.contains(&x_weekday) {
        return its_a_business_date_error_message(x);
    };

    let mut n_days =
        calculate_n_days_without_holidays_slow(x_weekday, n, n_weekdays, cache);

    if holidays.binary_search(&x).is_ok() {
        return its_a_business_date_error_message(x);
    }
    let mut count_hols = count_holidays(x, x + n_days, holidays);
    while count_hols > 0 {
        let n_days_before = n_days;
        if n_days > 0 {
            n_days = n_days
                + calculate_n_days_without_holidays_slow(
                    weekday(x_mod_7 + n_days),
                    count_hols,
                    n_weekdays,
                    cache,
                );
            count_hols = count_holidays(x + n_days_before + 1, x + n_days, holidays);
        } else {
            n_days = n_days
                + calculate_n_days_without_holidays_slow(
                    weekday(x_mod_7 + n_days),
                    -count_hols,
                    n_weekdays,
                    cache,
                );
            count_hols = count_holidays(x + n_days_before - 1, x + n_days, holidays);
        }
    }
    Ok(n_days)
}

pub(crate) fn calculate_n_days_with_weekend(
    x: i32,
    _x_mod_7: i32,
    n: i32,
    x_weekday: i32,
    weekend: &[i32],
    cache: Option<&AHashMap<i32, i32>>,
    _holidays: &[i32],
) -> PolarsResult<i32> {
    let cache = cache.unwrap();
    let n_weekdays = 7 - weekend.len() as i32;

    if weekend.contains(&x_weekday) {
        return its_a_business_date_error_message(x);
    };

    Ok(calculate_n_days_without_holidays_slow(
        x_weekday, n, n_weekdays, cache,
    ))
}

fn count_holidays(start: i32, end: i32, holidays: &[i32]) -> i32 {
    if end >= start {
        let start_pos = match holidays.binary_search(&start) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let end_pos = match holidays.binary_search(&end) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        };
        end_pos as i32 - start_pos as i32
    } else {
        let start_pos = match holidays.binary_search(&end) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let end_pos = match holidays.binary_search(&start) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        };
        end_pos as i32 - start_pos as i32
    }
}

pub(crate) fn impl_advance_n_days(
    s: &Series,
    n: &Series,
    holidays: Vec<i32>,
    weekend: Vec<i32>,
) -> PolarsResult<Series> {
    let original_dtype = s.dtype();

    // Set up weeekend cache.
    let n_weekend = weekend.len() as i32;
    let capacity = ((7 - n_weekend) * 2 + 1) * (7-n_weekend);
    let cache: Option<AHashMap<i32, i32>> = if weekend == [5, 6] {
        None
    } else {
        let mut cache: AHashMap<i32, i32> = AHashMap::with_capacity(capacity as usize);
        for x_weekday in 0..=6 {
            if weekend.contains(&x_weekday) {
                continue;
            }
            for n_days in (-(7 - n_weekend))..=(7 - n_weekend) {
                if n_days == 0 {
                    cache.insert((10 * n_days + x_weekday), 0);
                }
                let value = advance_few_days(x_weekday, n_days, &weekend);
                cache.insert((10 * n_days + x_weekday), value);
            }
        }
        Some(cache)
    };

    // Only keep holidays which aren't on weekends.
    let holidays: Vec<i32> = if weekend == [5, 6] {
        holidays.into_iter().filter(|x| weekday(*x) < 5).collect()
    } else {
        holidays
            .into_iter()
            .filter(|x| !weekend.contains(&weekday(*x)))
            .collect()
    };

    let n = n.i32()?;

    let calculate_advance = match (weekend == [5, 6], holidays.is_empty()) {
        (true, true) => {
            calculate_n_days_without_holidays_fast
        },
        (true, false) => {
            calculate_n_days_with_holidays
        },
        (false, true) => {
            calculate_n_days_with_weekend
        },
        (false, false) => {
            calculate_n_days_with_weekend_and_holidays
        }
    };



    match s.dtype() {
        DataType::Date => {
            let ca = s.date()?;
            let out = match n.len() {
                1 => {
                    if let Some(n) = n.get(0) {
                        ca.try_apply(|x_date| {
                            let x_mod_7 = x_date % 7;
                            let x_weekday = weekday(x_mod_7);
                            Ok(x_date+calculate_advance(x_date, x_mod_7, n, x_weekday, &weekend, cache.as_ref(), &holidays )?)
                        })
                    } else {
                        Ok(Int32Chunked::full_null(ca.name(), ca.len()))
                    }
                }
                _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
                    (Some(x_date), Some(n)) => {
                        let x_mod_7 = x_date % 7;
                        let x_weekday = weekday(x_mod_7);
                        Ok(x_date+calculate_advance(x_date, x_mod_7, n, x_weekday, &weekend, cache.as_ref(), &holidays )?).map(Some)
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
                &Utf8Chunked::from_iter(std::iter::once("raise")),
            )?;
            let out = match n.len() {
                1 => {
                    if let Some(n) = n.get(0) {
                        ca.try_apply(|x| {
                            let x_date = (x / multiplier) as i32;
                            let x_mod_7 = x_date % 7;
                            let x_weekday = weekday(x_mod_7);
                            Ok(x+(calculate_advance(x_date, x_mod_7, n, x_weekday, &weekend, cache.as_ref(), &holidays )? as i64 *multiplier))
                        })
                    } else {
                        Ok(Int64Chunked::full_null(ca.name(), ca.len()))
                    }
                }
                _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
                    (Some(x), Some(n)) => {
                        let x_date = (x / multiplier) as i32;
                        let x_mod_7 = x_date % 7;
                        let x_weekday = weekday(x_mod_7);
                        Ok(x+(calculate_advance(x_date, x_mod_7, n, x_weekday, &weekend, cache.as_ref(), &holidays )? as i64 *multiplier)).map(Some)
                    }
                    _ => Ok(None),
                }),
            };
            let out = polars_ops::prelude::replace_time_zone(
                &out?.into_datetime(*time_unit, None),
                time_zone.as_deref(),
                &Utf8Chunked::from_iter(std::iter::once("raise")),
            )?;
            out.cast(original_dtype)
        }
        _ => {
            polars_bail!(ComputeError: format!("expected Datetime or Date dtype, got: {}", original_dtype))
        }
    }
}
