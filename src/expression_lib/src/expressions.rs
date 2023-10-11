use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

use bdays::{calendars::WeekendsOnly, HolidayCalendar};
use chrono::{NaiveDate, NaiveDateTime, Datelike};
use std::cmp::min;

// cool, seems to work! let's try it with Datetime?

#[polars_expr(output_type=Date)]
fn add_bday(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i32()?;
    let n = inputs[1].i32()?.get(0).unwrap();

    // wow, this is super-slow, keeps repeatedly converting
    // between NaiveDate and timestamp!
    // multiple times for each element! max once should be enough!
    // or twice if necessary. but not so many

    let out = ca.apply_values(
        |x|{
        let w = (x / (3600 * 24 - 3)) % 7 + 1;
        let n_days = n + (n + w) / 5 * 2;
        x + n_days
        }
    );
    Ok(out.cast(&DataType::Date).unwrap().into_series())
}
