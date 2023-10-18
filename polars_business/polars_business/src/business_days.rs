use polars::prelude::*;
use polars::prelude::arity::try_binary_elementwise;
use ahash::AHashMap;

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
    n_weekend: i32,
    cache: &AHashMap<i32, i32>,
) -> i32 {
    let n_weeks = n / (7 - n_weekend);
    let n_days = n % (7 - n_weekend);
    let n_days = cache.get(&(n_days * 10 + x_weekday)).unwrap();
    n_days + n_weeks * 7
}

pub(crate) fn calculate_n_days_without_holidays_fast(n: i32, x_weekday: i32) -> i32 {
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

pub(crate) fn calculate_n_days_with_holidays(x: i32, n: i32, holidays: &[i32]) -> PolarsResult<i32> {
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
            n_days = n_days
                + calculate_n_days_without_holidays_fast(count_hols, weekday(x_mod_7 + n_days));
            count_hols = count_holidays(x + n_days_before + 1, x + n_days, &holidays);
        } else {
            n_days = n_days
                + calculate_n_days_without_holidays_fast(-count_hols, weekday(x_mod_7 + n_days));
            count_hols = count_holidays(x + n_days_before - 1, x + n_days, &holidays);
        }
    }
    Ok(n_days)
}

pub(crate) fn calculate_n_days_with_weekend_and_holidays(
    x: i32,
    n: i32,
    weekend: &[i32],
    cache: &AHashMap<i32, i32>,
    holidays: &[i32],
) -> PolarsResult<i32> {
    let x_mod_7 = x % 7;
    let x_weekday = weekday(x_mod_7);

    if weekend.contains(&x_weekday) {
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    };

    let mut n_days =
        calculate_n_days_without_holidays_slow(x_weekday, n, weekend.len() as i32, cache);

    if holidays.binary_search(&x).is_ok() {
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    }
    let mut count_hols = count_holidays(x, x + n_days, &holidays);
    while count_hols > 0 {
        let n_days_before = n_days;
        if n_days > 0 {
            n_days = n_days
                + calculate_n_days_without_holidays_slow(
                    weekday(x_mod_7 + n_days),
                    count_hols,
                    weekend.len() as i32,
                    cache,
                );
            count_hols = count_holidays(x + n_days_before + 1, x + n_days, &holidays);
        } else {
            n_days = n_days
                + calculate_n_days_without_holidays_slow(
                    weekday(x_mod_7 + n_days),
                    -count_hols,
                    weekend.len() as i32,
                    cache,
                );
            count_hols = count_holidays(x + n_days_before - 1, x + n_days, &holidays);
        }
    }
    Ok(n_days)
}

pub(crate) fn calculate_n_days_with_weekend(
    x: i32,
    n: i32,
    weekend: &[i32],
    cache: &AHashMap<i32, i32>,
) -> PolarsResult<i32> {
    let x_mod_7 = x % 7;
    let x_weekday = weekday(x_mod_7);
    let n_weekend = weekend.len() as i32;

    if weekend.contains(&x_weekday) {
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    };

    Ok(calculate_n_days_without_holidays_slow(
        x_weekday, n, n_weekend, cache,
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
    let capacity = ((7 - n_weekend) * 2 + 1) * 7;
    let cache: Option<AHashMap<i32, i32>> = if weekend == [5, 6] {
        None
    } else {
        let mut cache: AHashMap<i32, i32> = AHashMap::with_capacity(capacity as usize);
        for x_weekday in 0..=6 {
            for n_days in (-(7 - n_weekend))..=(7 - n_weekend) {
                let value = advance_few_days(x_weekday, n_days, &weekend);
                cache.insert(10 * n_days + x_weekday, value);
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

    match s.dtype() {
        DataType::Date => {
            let ca = s.date()?;
            let out = match n.len() {
                1 => {
                    if let Some(n) = n.get(0) {
                        if holidays.is_empty() && weekend == [5, 6] {
                            ca.try_apply(|x_date| {
                                let x_weekday = weekday(x_date);
                                if x_weekday >= 5 {
                                    polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x_date))
                                }
                                Ok(x_date+(calculate_n_days_without_holidays_fast(n, x_weekday)))
                            })
                        } else if !holidays.is_empty() && weekend == [5, 6] {
                            ca.try_apply(|x_date| {
                                Ok(x_date + calculate_n_days_with_holidays(x_date, n, &holidays)?)
                            })
                        } else if holidays.is_empty() && weekend != [5, 6] {
                            let cache = cache.unwrap();
                            ca.try_apply(|x_date| {
                                Ok(x_date
                                    + calculate_n_days_with_weekend(x_date, n, &weekend, &cache)?)
                            })
                        } else {
                            let cache = cache.unwrap();
                            ca.try_apply(|x_date| {
                                Ok(x_date
                                    + calculate_n_days_with_weekend_and_holidays(
                                        x_date, n, &weekend, &cache, &holidays,
                                    )?)
                            })
                        }
                    } else {
                        Ok(Int32Chunked::full_null(ca.name(), ca.len()))
                    }
                }
                _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
                    (Some(x_date), Some(n)) => {
                        if holidays.is_empty() && weekend == [5, 6] {
                            let x_weekday = weekday(x_date);
                            if x_weekday >= 5 {
                                polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x_date))
                            }
                            Ok(x_date + (calculate_n_days_without_holidays_fast(n, x_weekday)))
                                .map(Some)
                        } else if !holidays.is_empty() && weekend == [5, 6] {
                            Ok(x_date + calculate_n_days_with_holidays(x_date, n, &holidays)?)
                                .map(Some)
                        } else if holidays.is_empty() && weekend != [5, 6] {
                            let cache = cache.as_ref().unwrap();
                            Ok(x_date + calculate_n_days_with_weekend(x_date, n, &weekend, cache)?)
                                .map(Some)
                        } else {
                            let cache = cache.as_ref().unwrap();
                            Ok(x_date
                                + calculate_n_days_with_weekend_and_holidays(
                                    x_date, n, &weekend, &cache, &holidays,
                                )?)
                            .map(Some)
                        }
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
                        if holidays.is_empty() && weekend == [5, 6] {
                            ca.try_apply(|x| {
                                let x_date = (x / multiplier) as i32;
                                let x_weekday = weekday(x_date);
                                if x_weekday >= 5 {
                                    polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x_date))
                                }
                                Ok(x+(calculate_n_days_without_holidays_fast(n, x_weekday) as i64 *multiplier))
                            })
                        } else if !holidays.is_empty() && weekend == [5, 6] {
                            ca.try_apply(|x| {
                                let x_date = (x / multiplier) as i32;
                                Ok(
                                    x + calculate_n_days_with_holidays(x_date, n, &holidays)?
                                        as i64
                                        * multiplier,
                                )
                            })
                        } else if holidays.is_empty() && weekend != [5, 6] {
                            let cache = cache.unwrap();
                            ca.try_apply(|x| {
                                let x_date = (x / multiplier) as i32;
                                Ok(
                                    x + calculate_n_days_with_weekend(x_date, n, &weekend, &cache)?
                                        as i64
                                        * multiplier,
                                )
                            })
                        } else {
                            let cache = cache.unwrap();
                            ca.try_apply(|x| {
                                let x_date = (x / multiplier) as i32;
                                Ok(x + calculate_n_days_with_weekend_and_holidays(
                                    x_date, n, &weekend, &cache, &holidays,
                                )? as i64
                                    * multiplier)
                            })
                        }
                    } else {
                        Ok(Int64Chunked::full_null(ca.name(), ca.len()))
                    }
                }
                _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
                    (Some(x), Some(n)) => {
                        let x_date = (x / multiplier) as i32;
                        if holidays.is_empty() && weekend == [5, 6] {
                            let x_weekday = weekday(x_date);
                            if x_weekday >= 5 {
                                polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x_date))
                            }
                            Ok(x+(calculate_n_days_without_holidays_fast(n, x_weekday) as i64 *multiplier)).map(Some)
                        } else if !holidays.is_empty() && weekend == [5, 6] {
                            Ok(
                                x + calculate_n_days_with_holidays(x_date, n, &holidays)?
                                    as i64
                                    * multiplier,
                            ).map(Some)
                        } else if holidays.is_empty() && weekend != [5, 6] {
                            let cache = cache.as_ref().unwrap();
                            let x_date = (x / multiplier) as i32;
                            Ok(
                                x + calculate_n_days_with_weekend(x_date, n, &weekend, &cache)?
                                    as i64
                                    * multiplier,
                            ).map(Some)
                        } else {
                            let cache = cache.as_ref().unwrap();
                            Ok(x + calculate_n_days_with_weekend_and_holidays(
                                x_date, n, &weekend, &cache, &holidays,
                            )? as i64
                                * multiplier).map(Some)
                        }
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