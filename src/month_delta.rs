use chrono::{Datelike, NaiveDate};
use polars::prelude::*;

fn get_last_month_date(date: NaiveDate) -> NaiveDate {
    if date.month() == 12 {
        NaiveDate::from_ymd_opt(date.year() + 1, 1, 1)
            .unwrap()
            .pred_opt()
            .unwrap()
    } else {
        date.with_day(1)
            .unwrap()
            .with_month(date.month() + 1)
            .unwrap()
            .pred_opt()
            .unwrap()
    }
}

fn get_last_day_bool(start_date: NaiveDate, end_date: NaiveDate) -> bool {
    // Check if both dates fall on the last days of their respective months
    let end_date_end = get_last_month_date(end_date);
    let start_date_end = get_last_month_date(start_date);
    {
        // End date is the last day of its month
        end_date.day() == end_date_end.day() &&
            // Start date is the last day of its month
            start_date.day() == start_date_end.day() &&
            end_date.day() != start_date.day() &&
            start_date.month() != end_date.month()
    }
}

fn get_month_span_indicator(start_date: NaiveDate, end_date: NaiveDate) -> i32 {
    // Check 1: Check if the actual number of days difference matches
    // assuming both dates start on the first
    let actual_days_diff = end_date.signed_duration_since(start_date).num_days();
    let expected_days_diff = {
        let start_dt = start_date.with_day(1).unwrap(); // start date at the beginning of the month
        let end_dt = end_date.with_day(1).unwrap(); // end date at the beginning of a month
        end_dt.signed_duration_since(start_dt).num_days() // expected day difference as full months
    };

    // Calculates if the date difference spans entire months
    // If do then add additional month to the calculation
    if actual_days_diff == expected_days_diff
        && end_date.month() != start_date.month()
        && end_date.day() != start_date.day()
    {
        1
    } else if expected_days_diff.abs() > actual_days_diff.abs() {
        -1
    } else {
        0
    }
}

pub(crate) fn impl_month_delta(start_dates: &Series, end_dates: &Series) -> PolarsResult<Series> {
    if (start_dates.dtype() != &DataType::Date) || (end_dates.dtype() != &DataType::Date) {
        polars_bail!(InvalidOperation: "polars_xdt.month_delta only works on Date type. Please cast to Date first.");
    }
    let start_dates = start_dates.date()?;
    let end_dates = end_dates.date()?;

    let month_diff: Int32Chunked = start_dates
        .as_date_iter()
        .zip(end_dates.as_date_iter())
        .map(|(s_arr, e_arr)| {
            s_arr.zip(e_arr).map(|(start_date, end_date)| {
                let year_diff = end_date.year() - start_date.year();
                let mut month_diff = end_date.month() as i32 - start_date.month() as i32;
                month_diff += year_diff * 12;

                // Apply corrections based on the conditions checked earlier
                // Use absolute value to determine the magnitude of the change
                let mut abs_month_diff = month_diff.abs();

                abs_month_diff += get_month_span_indicator(start_date, end_date);

                if get_last_day_bool(start_date, end_date) {
                    // Add an extra month for end cases where both dates are at month-end
                    abs_month_diff += 1
                }

                // Return the final month difference
                // if start date is after the end date then return negative
                if month_diff < 0 {
                    -abs_month_diff
                } else {
                    abs_month_diff
                }
            })
        })
        .collect();

    Ok(month_diff.into_series())
}
