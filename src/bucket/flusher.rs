use std::{
    fs::File,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

use tracing::error;

#[derive(Clone, Debug)]
pub struct FlushSender(FlushSenderInner);

#[derive(Clone, Debug)]
enum FlushSenderInner {
    Local,
    Sender(mpsc::Sender<FlushMsg>),
}

impl FlushSender {
    /// Creates a flush sender which flushes synchronously,
    /// useful for testing or cases where you might not want a flusher thread running.
    pub fn local() -> Self {
        FlushSender(FlushSenderInner::Local)
    }

    pub(crate) fn flush_async(
        &self,
        file: &mut File,
        offset: u64,
        global_offset: &Arc<AtomicU64>,
    ) -> io::Result<()> {
        match &self.0 {
            FlushSenderInner::Local => {
                file.flush()?;
                global_offset.store(offset, Ordering::Release);
                Ok(())
            }
            FlushSenderInner::Sender(tx) => {
                let _ = tx.send(FlushMsg {
                    file: file.try_clone()?,
                    offset,
                    global_offset: Arc::clone(global_offset),
                });
                Ok(())
            }
        }
    }
}

struct FlushMsg {
    file: File,
    offset: u64,
    global_offset: Arc<AtomicU64>,
}

pub fn spawn_flusher() -> FlushSender {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || run_flusher(rx));

    FlushSender(FlushSenderInner::Sender(tx))
}

fn run_flusher(rx: mpsc::Receiver<FlushMsg>) {
    while let Ok(FlushMsg {
        mut file,
        offset,
        global_offset,
    }) = rx.recv()
    {
        match file.flush() {
            Ok(()) => {
                global_offset.store(offset, Ordering::Release);
            }
            Err(err) => {
                error!("failed to flush file: {err}");
            }
        }
    }
}
