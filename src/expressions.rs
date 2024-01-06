#![allow(clippy::unit_arg, clippy::unused_unit)]
use crate::business_days::*;
use crate::is_workday::*;
use crate::sub::*;
use crate::timezone::*;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use chrono::TimeZone;
use polars_arrow::array::{MutableUtf8Array, MutableArray, Utf8Array};
use std::fmt::Write;
#[derive(Deserialize)]
pub struct BusinessDayKwargs {
    holidays: Vec<i32>,
    weekmask: [bool; 7],
    roll: Option<String>,
}

#[derive(Deserialize)]
pub struct FromLocalDatetimeKwargs {
    to_tz: String,
    ambiguous: String,
}
#[derive(Deserialize)]
pub struct FormatLocalizedKwargs {
    format: String,
    locale: String,
}

fn bday_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = input_fields[0].clone();
    Ok(field)
}

pub fn to_local_datetime_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = input_fields[0].clone();
    let dtype = match field.dtype {
        DataType::Datetime(unit, _) => DataType::Datetime(unit, None),
        _ => polars_bail!(InvalidOperation:
            "dtype '{}' not supported", field.dtype
        ),
    };
    Ok(Field::new(&field.name, dtype))
}

pub fn from_local_datetime_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = input_fields[0].clone();
    let dtype = match field.dtype {
        DataType::Datetime(unit, _) => DataType::Datetime(unit, None),
        _ => polars_bail!(InvalidOperation:
            "dtype '{}' not supported", field.dtype
        ),
    };
    Ok(Field::new(&field.name, dtype))
}

#[polars_expr(output_type_func=bday_output)]
fn advance_n_days(inputs: &[Series], kwargs: BusinessDayKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let n = &inputs[1].cast(&DataType::Int32)?;
    let weekmask = kwargs.weekmask;
    let holidays = kwargs.holidays;
    let roll = kwargs.roll.unwrap();

    impl_advance_n_days(s, n, holidays, &weekmask, &roll)
}

#[polars_expr(output_type=Int32)]
fn sub(inputs: &[Series], kwargs: BusinessDayKwargs) -> PolarsResult<Series> {
    let begin_dates = &inputs[0];
    let end_dates = &inputs[1];
    let weekmask = kwargs.weekmask;
    let holidays = kwargs.holidays;
    impl_sub(begin_dates, end_dates, &weekmask, holidays)
}

#[polars_expr(output_type=Boolean)]
fn is_workday(inputs: &[Series], kwargs: BusinessDayKwargs) -> PolarsResult<Series> {
    let dates = &inputs[0];
    let weekmask = kwargs.weekmask;
    let holidays = kwargs.holidays;
    impl_is_workday(dates, &weekmask, &holidays)
}

#[polars_expr(output_type_func=to_local_datetime_output)]
fn to_local_datetime(inputs: &[Series]) -> PolarsResult<Series> {
    let s1 = &inputs[0];
    let ca = s1.datetime()?;
    let s2 = &inputs[1].str()?;
    Ok(elementwise_to_local_datetime(ca, s2)?.into_series())
}

#[polars_expr(output_type_func=from_local_datetime_output)]
fn from_local_datetime(inputs: &[Series], kwargs: FromLocalDatetimeKwargs) -> PolarsResult<Series> {
    let s1 = &inputs[0];
    let ca = s1.datetime().unwrap();
    let s2 = &inputs[1].str().unwrap();
    Ok(elementwise_from_local_datetime(ca, s2, &kwargs.to_tz, &kwargs.ambiguous)?.into_series())
}

#[polars_expr(output_type=String)]
fn format_localized(inputs: &[Series], kwargs: FormatLocalizedKwargs) -> PolarsResult<Series> {
    let s1 = &inputs[0];
    let ca = s1.datetime()?;
    let ndt = chrono::NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
    let dt = chrono::Utc.from_utc_datetime(&ndt);
    let format = kwargs.format;
    let locale = chrono::Locale::try_from(kwargs.locale.as_str()).map_err(|_| polars_err!(ComputeError: format!("given locale {} could not be parsed", kwargs.locale)))?;
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