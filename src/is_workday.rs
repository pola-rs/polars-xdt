use crate::business_days::weekday;
use polars::prelude::*;

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
            let out: BooleanChunked =
                ca.apply_values_generic(|date| is_workday_date(date, weekmask, holidays));
            Ok(out.into_series())
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
            let out: BooleanChunked = ca.apply_values_generic(|date| {
                is_workday_date((date / multiplier) as i32, weekmask, holidays)
            });
            Ok(out.into_series())
        }
        _ => {
            polars_bail!(InvalidOperation: "polars_xdt is_workday only works on Date/Datetime type.")
        }
    }
}
