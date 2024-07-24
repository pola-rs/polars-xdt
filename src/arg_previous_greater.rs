use polars::prelude::*;

pub(crate) fn impl_arg_previous_greater<T>(ca: &ChunkedArray<T>) -> IdxCa
where
    T: PolarsNumericType,
{
    let mut idx: Vec<i32> = Vec::with_capacity(ca.len());
    let out: IdxCa = ca
        .iter()
        .enumerate()
        .map(|(i, opt_val)| {
            if opt_val.is_none() {
                idx.push(-1);
                return None;
            }
            let i_curr = i;
            let mut i = (i as i32) - 1; // look at previous element
            while i >= 0 && ca.get(i as usize).is_none() {
                // find previous non-null value
                i -= 1;
            }
            if i < 0 {
                idx.push(-1);
                return None;
            }
            while (i != -1) && opt_val >= ca.get(i as usize) {
                i = idx[i as usize];
            }
            if i == -1 {
                idx.push(-1);
                return Some(i_curr as IdxSize);
            }
            idx.push(i);
            if i == -1 {
                None
            } else {
                Some(i as IdxSize)
            }
        })
        .collect();
    out
}
