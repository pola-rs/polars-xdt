mod business_days;
mod expressions;
mod is_workday;
mod sub;
mod timezone;

use pyo3::types::PyModule;
use pyo3::{pymodule, PyResult, Python};

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
