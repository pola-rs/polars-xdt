use crate::business_days::weekday;
use polars::prelude::*;
use pyo3_polars::export::polars_core::utils::{arrow::array::BooleanArray, CustomIterTools};

fn is_workday_date(date: i32, weekmask: &[bool; 7], holidays: &[i32]) -> bool {
    return unsafe { *weekmask.get_unchecked(weekday(date) as usize - 1) }
        && (!holidays.contains(&date));
}

pub(crate) fn impl_is_workday(
    dates: &Series,
    weekmask: &[bool; 7],
    holidays: &[i32],
) -> PolarsResult<Series> {
    match dates.dtype() {
        DataType::Date => {
            let ca = dates.date()?;
            let chunks = ca.downcast_iter().map(|arr| -> BooleanArray {
                arr.into_iter()
                    .map(|date| date.map(|date| is_workday_date(*date, weekmask, holidays)))
                    .collect_trusted()
            });
            Ok(BooleanChunked::from_chunk_iter(ca.name(), chunks).into_series())
        }
        DataType::Datetime(time_unit, _time_zone) => {
            let multiplier = match time_unit {
                TimeUnit::Milliseconds => 60 * 60 * 24 * 1_000,
                TimeUnit::Microseconds => 60 * 60 * 24 * 1_000_000,
                TimeUnit::Nanoseconds => 60 * 60 * 24 * 1_000_000_000,
            };
            let ca = &polars_ops::prelude::replace_time_zone(
                dates.datetime()?,
                None,
                &StringChunked::from_iter(std::iter::once("raise")),
            )?;
            let chunks = ca.downcast_iter().map(|arr| -> BooleanArray {
                arr.into_iter()
                    .map(|date| {
                        date.map(|date| {
                            is_workday_date((*date / multiplier) as i32, weekmask, holidays)
                        })
                    })
                    .collect_trusted()
            });
            Ok(BooleanChunked::from_chunk_iter(ca.name(), chunks).into_series())
        }
        _ => {
            polars_bail!(InvalidOperation: "polars_xdt is_workday currently only works on Date type. \
            For now, please cast to Date first.")
        }
    }
}
