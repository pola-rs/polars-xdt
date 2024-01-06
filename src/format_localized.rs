use chrono::format::StrftimeItems;
use chrono::TimeZone;
use chrono::{self, format::DelayedFormat};
use polars::prelude::*;
use polars_arrow::array::{MutableArray, MutableUtf8Array, Utf8Array};
use std::fmt::Write;
use std::str::FromStr;

fn timestamp_ms_to_datetime(timestamp: i64) -> chrono::NaiveDateTime {
    // Just unwrap because we know that the timestamp is from a valid datetime
    chrono::NaiveDateTime::from_timestamp_opt(
        timestamp / 1_000,
        (timestamp % 1_000 * 1_000_000) as u32,
    )
    .unwrap()
}
fn timestamp_us_to_datetime(timestamp: i64) -> chrono::NaiveDateTime {
    chrono::NaiveDateTime::from_timestamp_opt(
        timestamp / 1_000_000,
        (timestamp % 1_000_000 * 1_000) as u32,
    )
    .unwrap()
}
fn timestamp_ns_to_datetime(timestamp: i64) -> chrono::NaiveDateTime {
    chrono::NaiveDateTime::from_timestamp_opt(
        timestamp / 1_000_000_000,
        (timestamp % 1_000_000_000) as u32,
    )
    .unwrap()
}

pub(crate) fn impl_format_localized(
    s: &Series,
    format: &str,
    locale: &str,
) -> PolarsResult<Series> {
    let ndt = chrono::NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
    let dt = chrono::Utc.from_utc_datetime(&ndt);
    let locale = chrono::Locale::try_from(locale).map_err(
        |_| polars_err!(ComputeError: format!("given locale {} could not be parsed", locale)),
    )?;
    let fmted = format!("{}", dt.format_localized(format, locale));
    let name = s.name();

    match s.dtype() {
        DataType::Date => {
            let ca = s.date()?;
            fn format_ndt(
                ndt: chrono::NaiveDateTime,
                format: &str,
                locale: chrono::prelude::Locale,
                tz: chrono_tz::Tz,
            ) -> DelayedFormat<StrftimeItems<'_>> {
                let dt = tz.from_utc_datetime(&ndt);
                dt.format_localized(format, locale)
            }
            let mut ca: StringChunked = ca.apply_kernel_cast(&|arr| {
                let mut buf = String::new();
                let mut mutarr =
                    MutableUtf8Array::with_capacities(arr.len(), arr.len() * fmted.len() + 1);

                for opt in arr.into_iter() {
                    match opt {
                        None => mutarr.push_null(),
                        Some(timestamp) => {
                            buf.clear();
                            let ndt = timestamp_ms_to_datetime(
                                (*timestamp as i64) * 1_000 * 60 * 60 * 24,
                            );
                            let fmted = format_ndt(ndt, format, locale, chrono_tz::UTC);
                            write!(buf, "{fmted}").unwrap();
                            mutarr.push(Some(&buf))
                        }
                    }
                }

                let arr: Utf8Array<i64> = mutarr.into();
                Box::new(arr)
            });
            ca.rename(name);
            Ok(ca.into_series())
        }
        DataType::Datetime(time_unit, time_zone) => {
            let ca = s.datetime()?;
            let timestamp_to_datetime = match time_unit {
                TimeUnit::Nanoseconds => timestamp_ns_to_datetime,
                TimeUnit::Microseconds => timestamp_us_to_datetime,
                TimeUnit::Milliseconds => timestamp_ms_to_datetime,
            };
            let tz = match time_zone {
                None => chrono_tz::UTC,
                Some(tz) => chrono_tz::Tz::from_str(tz).unwrap(),
            };
            fn format_ndt(
                ndt: chrono::NaiveDateTime,
                format: &str,
                locale: chrono::prelude::Locale,
                tz: chrono_tz::Tz,
            ) -> DelayedFormat<StrftimeItems<'_>> {
                let dt = tz.from_utc_datetime(&ndt);
                dt.format_localized(format, locale)
            }
            let mut ca: StringChunked = ca.apply_kernel_cast(&|arr| {
                let mut buf = String::new();
                let mut mutarr =
                    MutableUtf8Array::with_capacities(arr.len(), arr.len() * fmted.len() + 1);

                for opt in arr.into_iter() {
                    match opt {
                        None => mutarr.push_null(),
                        Some(timestamp) => {
                            buf.clear();
                            let ndt = timestamp_to_datetime(*timestamp);
                            let fmted = format_ndt(ndt, format, locale, tz);
                            write!(buf, "{fmted}").unwrap();
                            mutarr.push(Some(&buf))
                        }
                    }
                }

                let arr: Utf8Array<i64> = mutarr.into();
                Box::new(arr)
            });
            ca.rename(name);
            Ok(ca.into_series())
        }
        _ => unreachable!(),
    }
}
