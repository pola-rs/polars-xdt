use polars::prelude::*;

pub(crate) fn impl_ewm_time(
    times: &Int64Chunked,
    values: &Float64Chunked,
    halflife: i64,
) -> Float64Chunked {
    let mut old_wt = 1.;
    let mut prev_time: i64 = 0;
    let mut prev_result: f64 = 0.;
    values.iter().zip(times.iter()).enumerate().map(|(i, (value, time))| {
        match (time, value) {
            (Some(time), Some(value)) => {
                let mut result: f64;
                if i == 0 {
                    result = value;
                }
                else {
                    let delta = (time - prev_time) as f64 / halflife as f64;
                    old_wt *= (0.5 as f64).powf(delta as f64);
                    result = old_wt * prev_result + value;
                    result /= old_wt + 1.;
                    old_wt += 1.;
                }
                prev_time = time;
                prev_result = result;
                Some(value)
            }
            _ => None,
        }
    }).collect()
}