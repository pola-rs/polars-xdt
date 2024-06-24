#![allow(clippy::unit_arg, clippy::unused_unit)]
use crate::arg_previous_greater::*;
use crate::format_localized::*;
use crate::month_delta::*;
use crate::timezone::*;
use crate::to_julian::*;
use crate::utc_offsets::*;
use chrono_tz::Tz;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use std::str::FromStr;

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


#[polars_expr(output_type=Int32)]
fn month_delta(inputs: &[Series]) -> PolarsResult<Series> {
    let start_dates = &inputs[0];
    let end_dates = &inputs[1];
    impl_month_delta(start_dates, end_dates)
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

fn list_idx_dtype(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = Field::new(input_fields[0].name(), DataType::List(Box::new(IDX_DTYPE)));
    Ok(field.clone())
}

#[polars_expr(output_type_func=list_idx_dtype)]
fn arg_previous_greater(inputs: &[Series]) -> PolarsResult<Series> {
    let ser = &inputs[0];
    match ser.dtype() {
        DataType::Int64 => Ok(impl_arg_previous_greater(ser.i64().unwrap()).into_series()),
        DataType::Int32 => Ok(impl_arg_previous_greater(ser.i32().unwrap()).into_series()),
        DataType::UInt64 => Ok(impl_arg_previous_greater(ser.u64().unwrap()).into_series()),
        DataType::UInt32 => Ok(impl_arg_previous_greater(ser.u32().unwrap()).into_series()),
        DataType::Float64 => Ok(impl_arg_previous_greater(ser.f64().unwrap()).into_series()),
        DataType::Float32 => Ok(impl_arg_previous_greater(ser.f32().unwrap()).into_series()),
        dt => polars_bail!(ComputeError:"Expected numeric data type, got: {}", dt),
    }
}
