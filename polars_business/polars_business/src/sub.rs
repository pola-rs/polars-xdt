use crate::business_days::weekday;
use polars::prelude::arity::binary_elementwise;
use polars::prelude::*;

fn date_diff(mut start_date: i32, mut end_date: i32) -> i32 {
    let swapped = start_date > end_date;
    if swapped {
        (start_date, end_date) = (end_date, start_date);
        start_date += 1;
        end_date += 1;
    }

    let mut start_weekday = weekday(start_date);
    let end_weekday = weekday(end_date);

    if start_weekday == 6 {
        start_date += 2;
        start_weekday = 1;
    } else if start_weekday == 7 {
        start_date += 1;
        start_weekday = 1;
    }
    if end_weekday == 6 {
        end_date += 2;
    } else if end_weekday == 7 {
        end_date += 1;
    }

    let diff = end_date - start_date;

    let whole_weeks = diff / 7;
    let mut count = 0;
    count += whole_weeks * 5;
    start_date += whole_weeks * 7;
    while start_date < end_date {
        if start_weekday < 6 {
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

pub(crate) fn impl_sub(
    end_dates: &Series,
    start_dates: &Series,
    // holidays: Vec<i32>,
    // weekend: Vec<i32>,
) -> PolarsResult<Series> {
    if (start_dates.dtype() != &DataType::Date) || (end_dates.dtype() != &DataType::Date) {
        polars_bail!(InvalidOperation: "polars_business sub only works on Date type. Please cast to Date first.");
    }
    let start_dates = start_dates.date()?;
    let end_dates = end_dates.date()?;
    let out = match end_dates.len() {
        1 => {
            if let Some(end_date) = end_dates.get(0) {
                start_dates.apply(|x_date| x_date.map(|start_date| date_diff(start_date, end_date)))
            } else {
                Int32Chunked::full_null(start_dates.name(), start_dates.len())
            }
        }
        _ => binary_elementwise(start_dates, end_dates, |opt_s, opt_n| {
            match (opt_s, opt_n) {
                (Some(start_date), Some(end_date)) => Some(date_diff(start_date, end_date)),
                _ => None,
            }
        }),
    };
    Ok(out.into_series())
}
