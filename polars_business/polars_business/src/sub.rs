use ahash::AHashMap;
use chrono::NaiveDateTime;
use polars::prelude::arity::binary_elementwise;
use polars::prelude::*;
use crate::business_days::weekday;

pub(crate) fn impl_sub(
    end_dates: &Series,
    start_dates: &Series,
    // holidays: Vec<i32>,
    // weekend: Vec<i32>,
) -> PolarsResult<Series> {
    // todo: raise if either is not Date?
    let start_dates = start_dates.date()?;
    let end_dates = end_dates.date()?;
    let out = match end_dates.len() {
        1 => {
            if let Some(end_date) = end_dates.get(0) {
                start_dates.apply(|x_date| {
                    // want to do:
                    // result=floor(row_number/6)
                    // result_np=min(floor(row_number/6),5)×6result_np=min(floor(row_number/6),5)×6
                    x_date.map(|x_date| {
                        end_date - ((x_date - 1)/5)*2
                    })
                })
            } else {
                Int32Chunked::full_null(start_dates.name(), start_dates.len())
            }
        }
        _ => binary_elementwise(start_dates, &end_dates, |opt_s, opt_n| match (opt_s, opt_n) {
            (Some(mut start_date), Some(mut end_date)) => {
                println!("***");
                let swapped = start_date > end_date;
                if swapped {
                    (start_date, end_date) = (end_date, start_date);
                    start_date += 1;
                    end_date += 1;
                }

                println!("start: {:?}", start_date);
                println!("end: {:?}", end_date);

                let mut start_weekday = weekday(start_date);
                let mut end_weekday = weekday(end_date);
                println!("start weekday: {:?}", start_weekday);
                println!("end weekday: {:?}", end_weekday);

                if start_weekday == 6 {
                    start_date += 2;
                    start_weekday = 1;
                } else if start_weekday == 7 {
                    start_date += 1;
                    start_weekday = 1;
                }
                if end_weekday == 6 {
                    end_date += 2;
                    end_weekday = 1;
                } else if end_weekday == 7 {
                    end_date += 1;
                    end_weekday = 1;
                }

                println!("start: {:?}", start_date);
                println!("end: {:?}", end_date);

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
                    Some(-count)
                } else {
                    Some(count)
                }
            }
            _ => None,
        }),
    };
    Ok(out.into_series())
}