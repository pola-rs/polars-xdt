use polars::prelude::arity::try_binary_elementwise;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn weekday(x: i32) -> i32 {
    // the first modulo might return a negative number, so we add 7 and take
    // the modulo again so we're sure we have something between 0 and 6
    ((x - 4) % 7 + 7) % 7
}

// wrong, but wip
fn advance_few_days(x_mod_7: i32, x_weekday: i32, n: i32, weekend: &[i32]) -> i32 {
    let mut n_days = 0;
    for _ in 0..n {
        n_days += 1;
        n_days = roll(n_days, weekday(x_mod_7+n_days), weekend);
    }
    n_days
}

fn calculate_n_days_without_holidays_slow(x_mod_7: i32, n: i32, x_weekday: i32, weekend: &[i32], n_weekend: i32) -> i32 {
    if n >= 0 {
        // ok, really need to rethink this
        // could do:
        // 1. how many whole weeks are we going forwards?
        //    ok, do that.
        // 2. just, go forwards the remaining days.
        //    forget this rolling forwards to monday thing

        let n_weeks = n / (7-n_weekend);

        let n_days = n % (7-n_weekend);
        let n_days = advance_few_days(x_mod_7, x_weekday, n_days, weekend);
        n_days + n_weeks * 7
    } else {
        -(-n + (-n + 4 - x_weekday) / 5 * 2)
    }
}

fn calculate_n_days_without_holidays_blazingly_fast(_x: i32, n: i32, _x_weekday: i32, _weekend: &[i32], _n_weekend: i32) -> i32 {
    n
}

fn calculate_n_days_without_holidays_fast(_x: i32, n: i32, x_weekday: i32, _weekend: &[i32], _n_weekend: i32) -> i32 {
    if n >= 0 {
        n + (n + x_weekday) / 5 * 2
    } else {
        -(-n + (-n + 4 - x_weekday) / 5 * 2)
    }
}

fn roll(n_days: i32, x_weekday: i32, weekend: &[i32]) -> i32 {
    let mut x_weekday = x_weekday;
    let mut n_days = n_days;
    while weekend.contains(&x_weekday) {
        x_weekday = (x_weekday + 1) % 7;
        n_days += 1;
    }
    n_days
}

fn roll(n_days: i32, weekday_res: i32) -> i32 {
    if n_days > 0 {
        if weekday_res == 5 {
            n_days + 2
        } else if weekday_res == 6 {
            n_days + 1
        } else {
            n_days
        }
    } else if weekday_res == 5 {
        n_days - 1
    } else if weekday_res == 6 {
        n_days - 2
    } else {
        n_days
    }
}

fn calculate_n_days(x: i32, n: i32, holidays: &[i32], weekend: &[i32]) -> PolarsResult<i32> {
    let x_mod_7 = x % 7;
    let x_weekday = weekday(x_mod_7);
    let len_weekend = weekend.len() as i32;

    if weekend.contains(&x_weekday) {
        polars_bail!(ComputeError: format!("date {} is not a business date, cannot advance. `roll` argument coming soon.", x))
    };

    let calculate_n_days = if weekend == vec![5, 6] {
        calculate_n_days_without_holidays_fast
    } else if weekend.is_empty() {
        calculate_n_days_without_holidays_blazingly_fast
    } else {
        calculate_n_days_without_holidays_slow
    };

    let mut n_days = calculate_n_days(x, n, x_weekday, weekend, len_weekend);

    if !holidays.is_empty() {
        if holidays.binary_search(&x).is_ok() {
            polars_bail!(ComputeError: "date is not a business date, cannot advance. `roll` argument coming soon.")
        }
        let mut count_hols = count_holidays(x, x + n_days, &holidays);
        while count_hols > 0 {
            let n_days_before = n_days;
            if n_days > 0 {
                n_days = n_days + calculate_n_days(x+n_days, count_hols, weekday(x_mod_7 + n_days), weekend, len_weekend);
                count_hols = count_holidays(x+n_days_before+1, x + n_days, &holidays);
            } else {
                n_days = n_days + calculate_n_days(x+n_days, -count_hols, weekday(x_mod_7 + n_days), weekend, len_weekend);
                count_hols = count_holidays(x+n_days_before-1, x + n_days, &holidays);
            }
        }
    };
    Ok(x + n_days)
}

fn count_holidays(start: i32, end: i32, holidays: &[i32]) -> i32 {
    if end >= start {
        let start_pos = match holidays.binary_search(&start) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let end_pos = match holidays.binary_search(&end) {
            Ok(pos) => pos+1,
            Err(pos) => pos,
        };
        end_pos as i32 - start_pos as i32
    } else {
        if start < holidays[0]{
            return 0
        }
        let start_pos = match holidays.binary_search(&end) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let end_pos = match holidays.binary_search(&start) {
            Ok(pos) => pos+1,
            Err(pos) => pos,
        };
        end_pos as i32 - start_pos as i32
    }
}

#[polars_expr(output_type=Date)]
fn advance_n_days(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i32()?;
    let n_series = inputs[1].cast(&DataType::Int32)?;
    let n = n_series.i32()?;

    let weekend = inputs[2].list()?.get(0).unwrap();
    let weekend: Vec<_> = Vec::from(weekend.i32()?).iter().filter_map(|&x| x).collect();

    let holidays = if inputs.len() == 4 {
        let binding = inputs[3].list()?.get(0).unwrap();
        let holidays = binding.i32()?;
        let mut vec: Vec<_> = Vec::from(holidays).iter().filter_map(|&x| x).collect();
        vec.sort();
        vec
    } else {
        Vec::new()
    };
    let holidays: Vec<_> = holidays
        .into_iter()
        .filter(|x| !weekend.contains(&weekday(*x)))
        .collect();


    let out = match n.len() {
        1 => {
            if let Some(n) = n.get(0) {
                ca.try_apply(|x| calculate_n_days(x, n, &holidays, &weekend))
            } else {
                Ok(Int32Chunked::full_null(ca.name(), ca.len()))
            }
        }
        _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
            (Some(s), Some(n)) => calculate_n_days(s, n, &holidays, &weekend).map(Some),
            _ => Ok(None),
        }),
    };

    out?.cast(&DataType::Date)
}
