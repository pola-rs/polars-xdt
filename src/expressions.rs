#![allow(clippy::unit_arg, clippy::unused_unit)]
use crate::business_days::*;
use crate::format_localized::*;
use crate::is_workday::*;
use crate::sub::*;
use crate::timezone::*;
use crate::to_julian::*;
use crate::utc_offsets::*;
use chrono_tz::Tz;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use std::str::FromStr;
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

fn same_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = input_fields[0].clone();
    Ok(field)
}
fn duration_ms(input_fields: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        input_fields[0].name(),
        DataType::Duration(TimeUnit::Milliseconds),
    ))
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

pub fn from_local_datetime_output(
    input_fields: &[Field],
    kwargs: FromLocalDatetimeKwargs,
) -> PolarsResult<Field> {
    let field = input_fields[0].clone();
    let dtype = match field.dtype {
        DataType::Datetime(unit, _) => DataType::Datetime(unit, Some(kwargs.to_tz)),
        _ => polars_bail!(InvalidOperation:
            "dtype '{}' not supported", field.dtype
        ),
    };
    Ok(Field::new(&field.name, dtype))
}

#[polars_expr(output_type_func=same_output)]
fn advance_n_days(inputs: &[Series], kwargs: BusinessDayKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let n = &inputs[1].cast(&DataType::Int32)?;
    let weekmask = kwargs.weekmask;
    let holidays = kwargs.holidays;
    let roll = kwargs.roll.unwrap();

    impl_advance_n_days(s, n, holidays, &weekmask, &roll)
}

#[polars_expr(output_type=Int32)]
fn workday_count(inputs: &[Series], kwargs: BusinessDayKwargs) -> PolarsResult<Series> {
    let begin_dates = &inputs[0];
    let end_dates = &inputs[1];
    let weekmask = kwargs.weekmask;
    let holidays = kwargs.holidays;
    impl_workday_count(begin_dates, end_dates, &weekmask, holidays)
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

#[polars_expr(output_type_func_with_kwargs=from_local_datetime_output)]
fn from_local_datetime(inputs: &[Series], kwargs: FromLocalDatetimeKwargs) -> PolarsResult<Series> {
    let s1 = &inputs[0];
    let ca = s1.datetime().unwrap();
    let s2 = &inputs[1].str().unwrap();
    Ok(elementwise_from_local_datetime(ca, s2, &kwargs.to_tz, &kwargs.ambiguous)?.into_series())
}

#[polars_expr(output_type=String)]
fn format_localized(inputs: &[Series], kwargs: FormatLocalizedKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let locale = kwargs.locale;
    let format = kwargs.format;
    impl_format_localized(s, &format, &locale)
}

#[polars_expr(output_type=Float64)]
fn to_julian_date(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    impl_to_julian_date(s)
}

#[polars_expr(output_type_func=duration_ms)]
fn base_utc_offset(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    match s.dtype() {
        DataType::Datetime(time_unit, Some(time_zone)) => {
            let time_zone = Tz::from_str(time_zone).unwrap();
            Ok(impl_base_utc_offset(s.datetime()?, time_unit, &time_zone).into_series())
        }
        _ => polars_bail!(InvalidOperation: "base_utc_offset only works on Datetime type."),
    }
}

#[polars_expr(output_type_func=duration_ms)]
fn dst_offset(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    match s.dtype() {
        DataType::Datetime(time_unit, Some(time_zone)) => {
            let time_zone = Tz::from_str(time_zone).unwrap();
            Ok(impl_dst_offset(s.datetime()?, time_unit, &time_zone).into_series())
        }
        _ => polars_bail!(InvalidOperation: "base_utc_offset only works on Datetime type."),
    }
}
