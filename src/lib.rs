mod arg_previous_greater;
mod expressions;
mod format_localized;
mod month_delta;
mod timezone;
mod to_julian;
mod utc_offsets;

use pyo3::types::{PyModule, PyModuleMethods};
use pyo3::{pymodule, Bound, PyResult};
use pyo3_polars::PolarsAllocator;

#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();

