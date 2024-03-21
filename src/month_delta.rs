use chrono::Datelike;
use chrono::NaiveDate;
use polars::prelude::*;

// Copied from https://docs.pola.rs/docs/rust/dev/src/polars_time/windows/duration.rs.html#398
// `add_month` is a private function.
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

/// Calculates the difference in months between two dates.
///
/// The difference is expressed as the number of whole months between the two dates.
/// If `right` is before `left`, the return value will be negative.
///
/// # Arguments
///
/// * `left`: `NaiveDate` - The start date.
/// * `right`: `NaiveDate` - The end date.
///
/// # Returns
///
/// * `i32` - The number of whole months between `left` and `right`.
///
/// # Examples
///
/// ```
/// let start_date = NaiveDate::from_ymd(2023, 1, 1);
/// let end_date = NaiveDate::from_ymd(2023, 4, 1);
/// assert_eq!(get_m_diff(start_date, end_date), 3);
/// ```
fn get_m_diff(left: NaiveDate, right: NaiveDate) -> i32 {
    let mut n = 0;
    if right >= left {
        if right.year() + 1 > left.year() {
            n = (right.year() - left.year() - 1) * 12;
        }
        while add_month(left, (n + 1).into()) <= right {
            n += 1;
        }
    } else {
        if left.year() + 1 > right.year() {
            n = -(left.year() - right.year() - 1) * 12;
        }
        while add_month(left, (n - 1).into()) >= right {
            n -= 1;
        }
    }
    n
}

/// Implements the month delta operation for Polars series containing dates.
///
/// This function calculates the difference in months between two series of dates.
/// The operation is pairwise: it computes the month difference for each pair
/// of start and end dates in the input series.
///
/// # Arguments
///
/// * `start_dates`: `&Series` - A series of start dates.
/// * `end_dates`: `&Series` - A series of end dates.
///
/// # Returns
///
/// * `PolarsResult<Series>` - A new series containing the month differences as `i32` values.
///
/// # Errors
///
/// Returns an error if the input series are not of the `Date` type.
///
/// # Examples
///
/// ```
/// use polars::prelude::*;
/// let date1 = NaiveDate::from_ymd(2023, 1, 1); // January 1, 2023
/// let date2 = NaiveDate::from_ymd(2023, 3, 1); // March 1, 2023
/// let date3 = NaiveDate::from_ymd(2023, 4, 1); // April 1, 2023
/// let date4 = NaiveDate::from_ymd(2023, 6, 1); // June 1, 2023
/// let start_dates = Series::new("start_dates", &[date1, date2]);
/// let end_dates = Series::new("end_dates", &[date3, date4]);
/// let month_deltas = impl_month_delta(&start_dates, &end_dates).unwrap();
/// ```
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
            s_arr
                .zip(e_arr)
                .map(|(start_date, end_date)| get_m_diff(start_date, end_date))
        })
        .collect();

    Ok(month_diff.into_series())
}
