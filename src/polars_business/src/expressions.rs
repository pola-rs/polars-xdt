use polars::prelude::arity::try_binary_elementwise;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn weekday(x: i32) -> i32 {
    // the first modulo might return a negative number, so we add 7 and take
    // the modulo again so we're sure we have something between 0 and 6
    ((x - 4) % 7 + 7) % 7
}

fn calculate_n_days_without_holidays(_x: i32, n: i32, x_weekday: i32) -> i32 {
    if n >= 0 {
        n + (n + x_weekday) / 5 * 2
    } else {
        -(-n + (-n + 4 - x_weekday) / 5 * 2)
    }
}

fn reduce_vec(vec: &[i32], x: i32, n_days: i32) -> Vec<i32> {
    // Each day we skip may be a holiday, and so require skipping an additional day.
    // n_days*2 is an upper-bound.
    if n_days > 0 {
        vec.iter()
            .copied()
            .filter(|&t| t >= x && t <= x + n_days * 2)
            .collect()
    } else {
        vec.iter()
            .copied()
            .filter(|&t| t <= x && t >= x + n_days * 2)
            .collect()
    }
}

fn increment_n_days(x: i32) -> i32 {
    if x > 0 {
        x + 1
    } else {
        x - 1
    }
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

fn calculate_n_days(x: i32, n: i32, vec: &Vec<i32>) -> PolarsResult<i32> {
    let x_mod_7 = x % 7;
    let x_weekday = weekday(x_mod_7);

    if x_weekday == 5 {
        polars_bail!(ComputeError: "Saturday is not a business date, cannot advance. `roll` argument coming soon.")
    } else if x_weekday == 6 {
        polars_bail!(ComputeError: "Sunday is not a business date, cannot advance. `roll` argument coming soon.")
    }

    let mut n_days = calculate_n_days_without_holidays(x, n, x_weekday);

    if !vec.is_empty() {
        let myvec: Vec<i32> = reduce_vec(vec, x, n_days);
        if !myvec.is_empty() {
            let mut count_hols = count_holidays(x, x + n_days, &myvec);
            while count_hols > 0 {
                let n_days_before = n_days;
                for _ in 0..count_hols {
                    n_days = increment_n_days(n_days);
                    let weekday_res = weekday(x_mod_7 + n_days);
                    n_days = roll(n_days, weekday_res);
                }
                if n_days_before > 0 {
                    count_hols = count_holidays(x+n_days_before+1, x + n_days, &myvec);
                } else {
                    count_hols = count_holidays(x+n_days_before-1, x + n_days, &myvec);
                }
            }
        }
    };
    Ok(x + n_days)
}

fn condition(x: i32, start: i32, end: i32) -> bool {
    if end > start {
        x >= start && x <= end
    } else {
        x <= start && x >= end
    }
}

fn count_holidays(start: i32, end: i32, holidays: &[i32]) -> i32 {
    holidays
        .iter()
        .filter(|&holiday| {
            condition(*holiday, start, end)
        })
        .count() as i32
}

#[polars_expr(output_type=Date)]
fn advance_n_days(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i32()?;
    let n_series = inputs[1].cast(&DataType::Int32)?;
    let n = n_series.i32()?;

    let vec = if inputs.len() == 3 {
        let binding = inputs[2].list()?.get(0).unwrap();
        let holidays = binding.i32()?;
        let mut vec: Vec<_> = Vec::from(holidays).iter().filter_map(|&x| x).collect();
        vec.sort();
        vec
    } else {
        Vec::new()
    };
    let vec: Vec<_> = vec
        .into_iter()
        .filter(|x| weekday(*x) < 5)
        .collect();

    let out = match n.len() {
        1 => {
            if let Some(n) = n.get(0) {
                ca.try_apply(|x| calculate_n_days(x, n, &vec))
            } else {
                Ok(Int32Chunked::full_null(ca.name(), ca.len()))
            }
        }
        _ => try_binary_elementwise(ca, n, |opt_s, opt_n| match (opt_s, opt_n) {
            (Some(s), Some(n)) => calculate_n_days(s, n, &vec).map(Some),
            _ => Ok(None),
        }),
    };

    out?.cast(&DataType::Date)
}
