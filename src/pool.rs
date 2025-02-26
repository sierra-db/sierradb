use std::process;

use rayon::{ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder};
use tracing::error;

use crate::error::ThreadPoolError;

pub fn create_thread_pool() -> Result<ThreadPool, ThreadPoolBuildError> {
    ThreadPoolBuilder::new()
        .panic_handler(|err| match err.downcast::<ThreadPoolError>() {
            Ok(err) => {
                error!("{err}");
                match *err {
                    ThreadPoolError::FlushEventIndex { .. } => {
                        // What to do when flushing an event index fails?
                        // For now, just exit.
                        process::exit(1);
                    }
                    ThreadPoolError::FlushStreamIndex { .. } => {
                        // What to do when flushing an stream index fails?
                        // For now, just exit.
                        process::exit(1);
                    }
                }
            }
            Err(err) => {
                if let Some(err) = err.downcast_ref::<&str>() {
                    error!("fatal error: {err}");
                    process::exit(1);
                }
                if let Some(err) = err.downcast_ref::<String>() {
                    error!("fatal error: {err}");
                    process::exit(1);
                }
                error!("unknown fatal error");
                process::exit(1);
            }
        })
        .build()
}
