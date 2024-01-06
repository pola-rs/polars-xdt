use arity::try_binary_elementwise;
use chrono::{LocalResult, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use polars::chunked_array::temporal::parse_time_zone;
use polars::prelude::*;
use pyo3_polars::export::polars_core::utils::arrow::legacy::kernels::Ambiguous;
use pyo3_polars::export::polars_core::utils::arrow::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
};
use std::str::FromStr;

fn naive_utc_to_naive_local_in_new_time_zone(
    from_tz: &Tz,
    to_tz: &Tz,
    ndt: NaiveDateTime,
) -> NaiveDateTime {
    // ndt is the UTC datetime corresponding to the datetime in from_tz timezone
    from_tz
        .from_utc_datetime(&ndt)
        .with_timezone(to_tz)
        .naive_local()
}

fn naive_local_to_naive_utc_in_new_time_zone(
    from_tz: &Tz,
    to_tz: &Tz,
    ndt: NaiveDateTime,
    ambiguous: &Ambiguous,
) -> PolarsResult<NaiveDateTime> {
    match from_tz.from_local_datetime(&ndt) {
        LocalResult::Single(dt) => Ok(dt.with_timezone(to_tz).naive_utc()),
        LocalResult::Ambiguous(dt_earliest, dt_latest) => match ambiguous {
            Ambiguous::Earliest => Ok(dt_earliest.with_timezone(to_tz).naive_utc()),
            Ambiguous::Latest => Ok(dt_latest.with_timezone(to_tz).naive_utc()),
            Ambiguous::Raise => {
                polars_bail!(ComputeError: "datetime '{}' is ambiguous in time zone '{}'. Please use `ambiguous` to tell how it should be localized.", ndt, to_tz)
            }
        },
        LocalResult::None => polars_bail!(ComputeError:
            "datetime '{}' is non-existent in time zone '{}'. Non-existent datetimes are not yet supported",
            ndt, to_tz
        ),
    }
}

pub fn elementwise_to_local_datetime(
    datetime: &Logical<DatetimeType, Int64Type>,
    tz: &StringChunked,
) -> PolarsResult<DatetimeChunked> {
    let from_time_zone = datetime.time_zone().as_deref().unwrap_or("UTC");
    let from_tz = parse_time_zone(from_time_zone)?;

    let timestamp_to_datetime: fn(i64) -> NaiveDateTime = match datetime.time_unit() {
        TimeUnit::Milliseconds => timestamp_ms_to_datetime,
        TimeUnit::Microseconds => timestamp_us_to_datetime,
        TimeUnit::Nanoseconds => timestamp_ns_to_datetime,
    };
    let datetime_to_timestamp: fn(NaiveDateTime) -> i64 = match datetime.time_unit() {
        TimeUnit::Milliseconds => datetime_to_timestamp_ms,
        TimeUnit::Microseconds => datetime_to_timestamp_us,
        TimeUnit::Nanoseconds => datetime_to_timestamp_ns,
    };
    let out: Result<ChunkedArray<Int64Type>, PolarsError> = match tz.len() {
        1 => match unsafe { tz.get_unchecked(0) } {
            Some(convert_tz) => {
                let to_tz = parse_time_zone(convert_tz)?;
                Ok(datetime.0.apply(|timestamp_opt| {
                    timestamp_opt.map(|ts| {
                        let ndt = timestamp_to_datetime(ts);
                        datetime_to_timestamp(naive_utc_to_naive_local_in_new_time_zone(
                            &from_tz, &to_tz, ndt,
                        ))
                    })
                }))
            }
            _ => Ok(datetime.0.apply(|_| None)),
        },
        _ => try_binary_elementwise(datetime, tz, |timestamp_opt, convert_tz_opt| {
            match (timestamp_opt, convert_tz_opt) {
                (Some(timestamp), Some(convert_tz)) => {
                    let ndt = timestamp_to_datetime(timestamp);
                    let to_tz = parse_time_zone(convert_tz)?;
                    Ok(Some(datetime_to_timestamp(
                        naive_utc_to_naive_local_in_new_time_zone(&from_tz, &to_tz, ndt),
                    )))
                }
                _ => Ok(None),
            }
        }),
    };
    let out = out?.into_datetime(datetime.time_unit(), None);
    Ok(out)
}

pub fn elementwise_from_local_datetime(
    datetime: &Logical<DatetimeType, Int64Type>,
    from_tz: &StringChunked,
    out_tz: &str,
    ambiguous: &str,
) -> PolarsResult<DatetimeChunked> {
    let to_tz = parse_time_zone(out_tz)?;
    let ambig = Ambiguous::from_str(ambiguous)?;
    let timestamp_to_datetime: fn(i64) -> NaiveDateTime = match datetime.time_unit() {
        TimeUnit::Milliseconds => timestamp_ms_to_datetime,
        TimeUnit::Microseconds => timestamp_us_to_datetime,
        TimeUnit::Nanoseconds => timestamp_ns_to_datetime,
    };
    let datetime_to_timestamp: fn(NaiveDateTime) -> i64 = match datetime.time_unit() {
        TimeUnit::Milliseconds => datetime_to_timestamp_ms,
        TimeUnit::Microseconds => datetime_to_timestamp_us,
        TimeUnit::Nanoseconds => datetime_to_timestamp_ns,
    };
    let out = match from_tz.len() {
        1 => match unsafe { from_tz.get_unchecked(0) } {
            Some(from_tz) => {
                let from_tz = parse_time_zone(from_tz)?;
                datetime.0.try_apply(|timestamp| {
                    let ndt = timestamp_to_datetime(timestamp);
                    Ok(datetime_to_timestamp(
                        naive_local_to_naive_utc_in_new_time_zone(&from_tz, &to_tz, ndt, &ambig)?,
                    ))
                })
            }
            _ => Ok(datetime.0.apply(|_| None)),
        },
        _ => try_binary_elementwise(datetime, from_tz, |timestamp_opt, from_tz_opt| {
            match (timestamp_opt, from_tz_opt) {
                (Some(timestamp), Some(from_tz)) => {
                    let ndt = timestamp_to_datetime(timestamp);
                    let from_tz = parse_time_zone(from_tz)?;
                    Ok(Some(datetime_to_timestamp(
                        naive_local_to_naive_utc_in_new_time_zone(&from_tz, &to_tz, ndt, &ambig)?,
                    )))
                }
                _ => Ok(None),
            }
        }),
    };
    let out = out?.into_datetime(datetime.time_unit(), Some(out_tz.to_string()));
    Ok(out)
}
