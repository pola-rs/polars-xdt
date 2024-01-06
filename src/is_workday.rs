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
            dates.downcast_iter().map(|arr| -> BooleanArray {
                arr.into_iter()
                    .map(|date| {
                        date.map(
                            |date|
                                unsafe { *weekmask.get_unchecked(weekday(*date) as usize - 1) }
                                && (!holidays.contains(date))
                        )
                    })
                    .collect_trusted()
            })
        }
        _ => {
            polars_bail!(InvalidOperation: "polars_xdt is_workday currently only works on Date type. \
            For now, please cast to Date first.")
        }
    };
    Ok(BooleanChunked::from_chunk_iter(dates.name(), out).into_series())
}
