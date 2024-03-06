use chrono::Datelike;
use chrono::NaiveDate;
use polars::prelude::*;

fn add_month(ts: NaiveDate, n_months: i64) -> NaiveDate {
    // Have to define, because it is hidden
    const DAYS_PER_MONTH: [[i64; 12]; 2] = [
        //J   F   M   A   M   J   J   A   S   O   N   D
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31], // non-leap year
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31], // leap year
    ];
    let months = n_months;

    // Retrieve the current date and increment the values
    // based on the number of months

    let mut year = ts.year();
    let mut month = ts.month() as i32;
    let mut day = ts.day();
    year += (months / 12) as i32;
    month += (months % 12) as i32;

    // if the month overflowed or underflowed, adjust the year
    // accordingly. Because we add the modulo for the months
    // the year will only adjust by one
    if month > 12 {
        year += 1;
        month -= 12;
    } else if month <= 0 {
        year -= 1;
        month += 12;
    }

    // Adding this not to import copy pasta again
    let leap_year = year % 400 == 0 || (year % 4 == 0 && year % 100 != 0);
    // Normalize the day if we are past the end of the month.
    let last_day_of_month = DAYS_PER_MONTH[leap_year as usize][(month - 1) as usize] as u32;

    if day > last_day_of_month {
        day = last_day_of_month
    }

    NaiveDate::from_ymd_opt(year, month as u32, day).unwrap()
}

fn get_m_diff(mut left: NaiveDate, right: NaiveDate) -> i32 {
    let mut n = 0;
    while left < right {
        left = add_month(left, 1);
        if left <= right {
            n += 1;
        }
    }
    n
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
                if start_date > end_date {
                    -get_m_diff(end_date, start_date)
                } else {
                    get_m_diff(start_date, end_date)
                }
            })
        })
        .collect();

    Ok(month_diff.into_series())
}
