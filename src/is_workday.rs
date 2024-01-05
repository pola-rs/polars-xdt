use crate::business_days::weekday;
use polars::prelude::*;
use pyo3_polars::export::polars_core::utils::{arrow::array::BooleanArray, CustomIterTools};

pub(crate) fn impl_is_workday(
    dates: &Series,
    weekmask: &[bool; 7],
    holidays: &[i32],
) -> PolarsResult<Series> {
    let out = match dates.dtype() {
        DataType::Date => {
            let dates = dates.date()?;
            dates.downcast_iter().map(
                |arr| -> BooleanArray {
                    arr.into_iter().map(|date| {
                        if let Some(date) = date {
                            let day_of_week = weekday(*date) as usize;
                            Some(unsafe { *weekmask.get_unchecked(day_of_week - 1) }
                                & (!holidays.contains(&date)))
                        } else {
                            None
                        }
                    }).collect_trusted()
                }
            )
        }
        _ => {
            polars_bail!(InvalidOperation: "polars_xdt is_workday currently only works on Date type. \
            For now, please cast to Date first.")
        }
    };
    Ok(BooleanChunked::from_chunk_iter(dates.name(), out).into_series())
}
