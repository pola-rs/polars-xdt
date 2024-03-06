use polars::prelude::*;
use polars_arrow::array::PrimitiveArray;

pub(crate) fn impl_ewma_by_time_float(
    times: &Int64Chunked,
    values: &Float64Chunked,
    half_life: i64,
    time_unit: TimeUnit,
) -> Float64Chunked {
    let mut out = Vec::with_capacity(times.len());
    if values.is_empty() {
        return Float64Chunked::full_null("", times.len());
    }

    let half_life = match time_unit {
        TimeUnit::Milliseconds => half_life / 1_000,
        TimeUnit::Microseconds => half_life,
        TimeUnit::Nanoseconds => half_life * 1_000,
    };

    let mut prev_time: i64 = times.get(0).unwrap();
    let mut prev_result = values.get(0).unwrap();
    out.push(Some(prev_result));
    values
        .iter()
        .zip(times.iter())
        .skip(1)
        .for_each(|(value, time)| {
            match (time, value) {
                (Some(time), Some(value)) => {
                    let delta_time = time - prev_time;
                    // equivalent to:
                    // alpha = exp(-delta_time*ln(2) / half_life)
                    let alpha = (0.5_f64).powf(delta_time as f64 / half_life as f64);
                    let result = (1. - alpha) * value + alpha * prev_result;
                    prev_time = time;
                    prev_result = result;
                    out.push(Some(result));
                }
                _ => out.push(None),
            }
        });
    let arr = PrimitiveArray::<f64>::from(out);
    Float64Chunked::from(arr)
}

pub(crate) fn impl_ewma_by_time(
    times: &Int64Chunked,
    values: &Series,
    half_life: i64,
    time_unit: TimeUnit,
) -> Series {
    match values.dtype() {
        DataType::Float64 => {
            let values = values.f64().unwrap();
            impl_ewma_by_time_float(times, values, half_life, time_unit).into_series()
        }
        DataType::Int64 | DataType::Int32 => {
            let values = values.cast(&DataType::Float64).unwrap();
            let values = values.f64().unwrap();
            impl_ewma_by_time_float(times, values, half_life, time_unit).into_series()
        }
        DataType::Float32 => {
            // todo: preserve Float32 in this case
            let values = values.cast(&DataType::Float64).unwrap();
            let values = values.f64().unwrap();
            impl_ewma_by_time_float(times, values, half_life, time_unit).into_series()
        }
        dt => panic!("Expected values to be signed numeric, got {:?}", dt),
    }
}
