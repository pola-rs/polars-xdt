// Δti+1 = ti+1 − ti
// qi+1 = exp(-Δti+1/λ)
// y0 = x0
// yi+1 = qi+1 × yi + (1 − qi+1) × xi+1
use polars::prelude::*;
use polars_arrow::array::PrimitiveArray;

pub(crate) fn impl_ewm_time(
    times: &Int64Chunked,
    values: &Float64Chunked,
    halflife: i64,
) -> Float64Chunked {
    let old_wt_factor: f64 = 0.5;
    let new_wt = 1.;
    let mut old_wt = 1.;

    let mut result: Vec<Option<f64>> = Vec::with_capacity(values.len());
    let mut prev_time: i64 = 0;
    let mut weighted: f64 = 0.;
    for (i, (value, time)) in values.iter().zip(times.iter()).enumerate() {
        match (time, value) {
            (Some(time), Some(value)) => {
                if i == 0 {
                    weighted = value;
                    result.push(Some(value));
                }
                else {
                    let delta = (time - prev_time) as f64 / halflife as f64;
                    old_wt *= old_wt_factor.powf(delta as f64);
                    weighted = old_wt * weighted + new_wt * value;
                    weighted /= old_wt + new_wt;
                    old_wt += new_wt;
                    result.push(Some(weighted));
                }
                prev_time = time;
            }
            _ => result.push(None),
        }
    }
    Float64Chunked::from(PrimitiveArray::<f64>::from(result))
}