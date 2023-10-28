use crate::business_days::weekday;
use polars::prelude::*;

pub(crate) fn impl_is_workday(
    dates: &Series,
    weekmask: &[bool; 7],
    holidays: &[i32],
) -> PolarsResult<Series> {
    let out = match dates.dtype() {
        DataType::Date => {
            let dates = dates.date()?;
            dates.apply(|x_date| {
                x_date.map(|date| {
                    let day_of_week = weekday(date) as usize;
                    match unsafe { *weekmask.get_unchecked(day_of_week - 1) }
                        & (!holidays.contains(&date))
                    {
                        true => 1,
                        false => 0,
                    }
                })
            })
        }
        _ => {
            polars_bail!(InvalidOperation: "polars_business is_workday currently only works on Date type. \
            For now, please cast to Date first.")
        }
    };
    Ok(out.cast(&DataType::Boolean)?.into_series())
}
