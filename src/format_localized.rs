use chrono::format::StrftimeItems;
use chrono::TimeZone;
use chrono::{self, format::DelayedFormat};
use polars::prelude::*;
use polars_arrow::array::MutablePlString;
use polars_arrow::temporal_conversions::MILLISECONDS_IN_DAY;
use polars_arrow::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
};
use std::fmt::Write;
use std::str::FromStr;

fn format_ndt(
    ndt: chrono::NaiveDateTime,
    format: &str,
    locale: chrono::prelude::Locale,
    tz: chrono_tz::Tz,
) -> DelayedFormat<StrftimeItems<'_>> {
    let dt = tz.from_utc_datetime(&ndt);
    dt.format_localized(format, locale)
}

pub(crate) fn impl_format_localized(
    s: &Series,
    format: &str,
    locale: &str,
) -> PolarsResult<Series> {
    let locale = chrono::Locale::try_from(locale).map_err(
        |_| polars_err!(ComputeError: format!("given locale {} could not be parsed", locale)),
    )?;
    let name = s.name();

    let mut ca: StringChunked = match s.dtype() {
        DataType::Date => {
            let ca = s.date()?;
            ca.apply_kernel_cast(&|arr| {
                let mut buf = String::new();
                let mut mutarr = MutablePlString::with_capacity(arr.len());

                for opt in arr.into_iter() {
                    match opt {
                        None => mutarr.push_null(),
                        Some(timestamp) => {
                            buf.clear();
                            let ndt =
                                timestamp_ms_to_datetime((*timestamp as i64) * MILLISECONDS_IN_DAY);
                            let fmted = format_ndt(ndt, format, locale, chrono_tz::UTC);
                            write!(buf, "{fmted}").unwrap();
                            mutarr.push(Some(&buf))
                        }
                    }
                }

                mutarr.freeze().boxed()
            })
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
            ca.apply_kernel_cast(&|arr| {
                let mut buf = String::new();
                let mut mutarr = MutablePlString::with_capacity(arr.len());

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

                mutarr.freeze().boxed()
            })
        }
        _ => unreachable!(),
    };
    ca.rename(name);
    Ok(ca.into_series())
}
