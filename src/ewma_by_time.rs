use polars::prelude::*;
use polars_arrow::array::PrimitiveArray;
use pyo3_polars::export::polars_core::export::num::Pow;

pub(crate) fn impl_ewma_by_time_float(
    times: &Int64Chunked,
    values: &Float64Chunked,
    halflife: i64,
    adjust: bool,
    time_unit: TimeUnit,
) -> Float64Chunked {
    let mut out = Vec::with_capacity(times.len());
    if values.is_empty() {
        return Float64Chunked::full_null("", times.len());
    }

    let halflife = match time_unit {
        TimeUnit::Milliseconds => halflife / 1_000,
        TimeUnit::Microseconds => halflife,
        TimeUnit::Nanoseconds => halflife * 1_000,
    };

    let mut prev_time: i64 = times.get(0).unwrap();
    let mut prev_result = values.get(0).unwrap();
    let mut prev_alpha = 0.0;
    out.push(Some(prev_result));
    let _ = values
        .iter()
        .zip(times.iter())
        .skip(1)
        .map(|(value, time)| {
            match (time, value) {
                (Some(time), Some(value)) => {
                    let delta_time = time - prev_time;
                    let result: f64;
                    if adjust {
                        let alpha =
                            (prev_alpha + 1.) * Pow::pow(0.5, delta_time as f64 / halflife as f64);
                        result = (value + alpha * prev_result) / (1. + alpha);
                        prev_alpha = alpha;
                    } else {
                        // equivalent to:
                        // alpha = exp(-delta_time*ln(2) / halflife)
                        prev_alpha = (0.5_f64).powf(delta_time as f64 / halflife as f64);
                        result = (1. - prev_alpha) * value + prev_alpha * prev_result;
                    }
                    prev_time = time;
                    prev_result = result;
                    out.push(Some(result));
                }
                _ => out.push(None),
            }
        })
        .collect::<Vec<_>>();
    let arr = PrimitiveArray::<f64>::from(out);
    Float64Chunked::from(arr)
}

pub(crate) fn impl_ewma_by_time(
    times: &Int64Chunked,
    values: &Series,
    halflife: i64,
    adjust: bool,
    time_unit: TimeUnit,
) -> Series {
    match values.dtype() {
        DataType::Float64 => {
            let values = values.f64().unwrap();
            impl_ewma_by_time_float(times, values, halflife, adjust, time_unit).into_series()
        }
        DataType::Int64 | DataType::Int32 => {
            let values = values.cast(&DataType::Float64).unwrap();
            let values = values.f64().unwrap();
            impl_ewma_by_time_float(times, values, halflife, adjust, time_unit).into_series()
        }
        DataType::Float32 => {
            // todo: preserve Float32 in this case
            let values = values.cast(&DataType::Float64).unwrap();
            let values = values.f64().unwrap();
            impl_ewma_by_time_float(times, values, halflife, adjust, time_unit).into_series()
        }
        dt => panic!("Expected values to be signed numeric, got {:?}", dt),
    }
}
