use chrono::{Datelike, Timelike};
use polars::prelude::*;
use polars_arrow::legacy::utils::CustomIterTools;
use polars_arrow::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
};
use polars_arrow::{array::Float64Array, temporal_conversions::MILLISECONDS_IN_DAY};

fn to_julian_date(
    mut year: i32,
    mut month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    nanosecond: u32,
) -> f64 {
    if month < 3 {
        year -= 1;
        month += 12;
    }
    day as f64 + (((153 * month - 457) / 5) as i64) as f64 + (365 * year) as f64 + (year / 4) as f64
        - (year / 100) as f64
        + (year / 400) as f64
        + 1_721_118.5
        + (hour as f64
            + minute as f64 / 60.
            + second as f64 / 3600.
            + nanosecond as f64 / 3600. / (10_i32.pow(9) as f64))
            / 24.
}

pub(crate) fn impl_to_julian_date(s: &Series) -> PolarsResult<Series> {
    match s.dtype() {
        DataType::Date => {
            let ca = s.date()?;
            let chunks = ca.downcast_iter().map(|arr| -> Float64Array {
                arr.into_iter()
                    .map(|timestamp_opt| {
                        timestamp_opt.map(|timestamp| {
                            let ndt =
                                timestamp_ms_to_datetime((*timestamp as i64) * MILLISECONDS_IN_DAY);
                            to_julian_date(ndt.year(), ndt.month(), ndt.day(), 0, 0, 0, 0)
                        })
                    })
                    .collect_trusted()
            });
            Ok(Float64Chunked::from_chunk_iter(ca.name(), chunks).into_series())
        }
        DataType::Datetime(time_unit, time_zone) => {
            if !(time_zone.is_none() || time_zone.as_deref() == Some("UTC")) {
                polars_bail!(InvalidOperation: "polars_xdt to_julian currently only works on UTC or naive Datetime type. \
                For now, please cast to UTC Datetime first.")
            };
            let timestamp_to_datetime = match time_unit {
                TimeUnit::Nanoseconds => timestamp_ns_to_datetime,
                TimeUnit::Microseconds => timestamp_us_to_datetime,
                TimeUnit::Milliseconds => timestamp_ms_to_datetime,
            };
            let ca = &polars_ops::prelude::replace_time_zone(
                s.datetime()?,
                None,
                &StringChunked::from_iter(std::iter::once("raise")),
            )?;
            let chunks = ca.downcast_iter().map(|arr| -> Float64Array {
                arr.into_iter()
                    .map(|timestamp_opt| {
                        timestamp_opt.map(|timestamp| {
                            let ndt = timestamp_to_datetime(*timestamp);
                            to_julian_date(
                                ndt.year(),
                                ndt.month(),
                                ndt.day(),
                                ndt.hour(),
                                ndt.minute(),
                                ndt.second(),
                                ndt.nanosecond(),
                            )
                        })
                    })
                    .collect_trusted()
            });
            Ok(Float64Chunked::from_chunk_iter(ca.name(), chunks).into_series())
        }
        _ => {
            polars_bail!(InvalidOperation: "polars_xdt to_julian currently only works on Date type. \
            For now, please cast to Date first.")
        }
    }
}
