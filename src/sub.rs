use crate::business_days::weekday;
use polars::prelude::arity::binary_elementwise;
use polars::prelude::*;

fn date_diff(
    mut start_date: i32,
    mut end_date: i32,
    weekmask: &[bool; 7],
    n_weekdays: i32,
    holidays: &[i32],
) -> i32 {
    let swapped = start_date > end_date;
    if swapped {
        (start_date, end_date) = (end_date, start_date);
        start_date += 1;
        end_date += 1;
    }

    let holidays_begin = match holidays.binary_search(&start_date) {
        Ok(x) => x,
        Err(x) => x,
    } as i32;
    let holidays_end = match holidays.binary_search(&end_date) {
        Ok(x) => x,
        Err(x) => x,
    } as i32;

    let mut start_weekday = weekday(start_date) as usize;
    let diff = end_date - start_date;
    let whole_weeks = diff / 7;
    let mut count = -(holidays_end - holidays_begin);
    count += whole_weeks * n_weekdays;
    start_date += whole_weeks * 7;
    while start_date < end_date {
        if unsafe { *weekmask.get_unchecked(start_weekday - 1) } {
            count += 1;
        }
        start_date += 1;
        start_weekday += 1;
        if start_weekday > 7 {
            start_weekday = 1;
        }
    }
    if swapped {
        -count
    } else {
        count
    }
}

pub(crate) fn impl_workday_count(
    start_dates: &Series,
    end_dates: &Series,
    weekmask: &[bool; 7],
    holidays: Vec<i32>,
) -> PolarsResult<Series> {
    if (start_dates.dtype() != &DataType::Date) || (end_dates.dtype() != &DataType::Date) {
        polars_bail!(InvalidOperation: "polars_xdt.workday_count only works on Date type. Please cast to Date first.");
    }
    // Only keep holidays which aren't on weekends.
    let holidays: Vec<i32> = {
        holidays
            .into_iter()
            .filter(|x| unsafe { *weekmask.get_unchecked(weekday(*x) as usize - 1) })
            .collect()
    };
    let start_dates = start_dates.date()?;
    let end_dates = end_dates.date()?;
    let n_weekdays = weekmask.iter().filter(|&x| *x).count() as i32;
    let out = match (start_dates.len(), end_dates.len()) {
        (_, 1) => {
            if let Some(end_date) = end_dates.get(0) {
                start_dates.apply(|x_date| {
                    x_date.map(|start_date| {
                        date_diff(start_date, end_date, weekmask, n_weekdays, &holidays)
                    })
                })
            } else {
                Int32Chunked::full_null(start_dates.name(), start_dates.len())
            }
        }
        (1, _) => {
            if let Some(start_date) = start_dates.get(0) {
                end_dates.apply(|x_date| {
                    x_date.map(|end_date| {
                        date_diff(start_date, end_date, weekmask, n_weekdays, &holidays)
                    })
                })
            } else {
                Int32Chunked::full_null(start_dates.name(), start_dates.len())
            }
        }
        _ => binary_elementwise(start_dates, end_dates, |opt_s, opt_n| {
            match (opt_s, opt_n) {
                (Some(start_date), Some(end_date)) => Some(date_diff(
                    start_date, end_date, weekmask, n_weekdays, &holidays,
                )),
                _ => None,
            }
        }),
    };
    Ok(out.into_series())
}
