use polars::prelude::*;

pub(crate) fn impl_arg_previous_greater<T>(ca: &ChunkedArray<T>) -> IdxCa
where
    T: PolarsNumericType,
{
    let mut idx: Vec<Option<i32>> = Vec::with_capacity(ca.len());
    let out: IdxCa = ca
        .into_iter()
        .enumerate()
        .map(|(i, opt_val)| {
            if opt_val.is_none() {
                idx.push(None);
                return None;
            }
            let i_curr = i;
            let mut i = Some((i as i32) - 1); // look at previous element
            while i >= Some(0) && ca.get(i.unwrap() as usize).is_none() {
                // find previous non-null value
                i = Some(i.unwrap() - 1)
            }
            if i < Some(0) {
                idx.push(None);
                return None;
            }
            while i.is_some() && opt_val >= ca.get(i.unwrap() as usize) {
                i = idx[i.unwrap() as usize];
            }
            if i.is_none() {
                idx.push(None);
                return Some(i_curr as IdxSize);
            }
            idx.push(i);
            i.map(|x| x as IdxSize)
        })
        .collect();
    out
}
