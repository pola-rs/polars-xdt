use polars::prelude::*;
use chrono;
use chrono::TimeZone;
use polars_arrow::array::{MutableUtf8Array, MutableArray, Utf8Array};
use std::fmt::Write;

pub(crate) fn impl_format_localized(
    s: &Series,
    format: &str,
    locale: &str,
) -> PolarsResult<Series> {
    let ca = s.datetime()?;
    let ndt = chrono::NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
    let dt = chrono::Utc.from_utc_datetime(&ndt);
    let locale = chrono::Locale::try_from(locale).map_err(|_| polars_err!(ComputeError: format!("given locale {} could not be parsed", locale)))?;
    let fmted = format!("{}", dt.format_localized(&format, locale));
    let name = ca.name();
    let mut ca: StringChunked = ca.apply_kernel_cast(&|arr| {
        let mut buf = String::new();
        let mut mutarr =
            MutableUtf8Array::with_capacities(arr.len(), arr.len() * fmted.len() + 1);

        for opt in arr.into_iter() {
            match opt {
                None => mutarr.push_null(),
                Some(timestamp) => {
                    buf.clear();
                    let ndt = chrono::NaiveDateTime::from_timestamp_opt(timestamp / 1_000_000, (timestamp % 1_000_000*1000)as u32).unwrap();
                    let dt = chrono::Utc.from_utc_datetime(&ndt);
                    let fmted = dt.format_localized(&format, locale);
                    write!(buf, "{fmted}").unwrap();
                    mutarr.push(Some(&buf))
                },
            }
        }

        let arr: Utf8Array<i64> = mutarr.into();
        Box::new(arr)
    });
    ca.rename(name);
    Ok(ca.into_series())
}