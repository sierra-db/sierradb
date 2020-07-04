use super::{reader::*, *};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

/// Number of bytes contained in the base name of the file.
pub static SEGMENT_FILE_NAME_LEN: usize = 20;
/// File extension for the segment file.
pub static SEGMENT_FILE_NAME_EXTENSION: &'static str = "log";

/// Magic that appears in the header of the segment for version 1.
///
/// There are a couple reasons for the magic. The primary reason is
/// to allow versioning, when the time comes. The second is to remove
/// the possibility of a 0 offset within the index. This helps to identity
/// the start of new index entries.
pub static VERSION_1_MAGIC: [u8; 2] = [0xff, 0xff];

#[derive(Debug)]
pub enum SegmentAppendError {
    LogFull,
    IoError(io::Error),
}

impl From<io::Error> for SegmentAppendError {
    #[inline]
    fn from(e: io::Error) -> SegmentAppendError {
        SegmentAppendError::IoError(e)
    }
}

pub struct AppendMetadata {
    pub starting_position: usize,
}

/// A segment is a portion of the commit log. Segments are append-only logs
/// written until the maximum size is reached.
pub struct Segment {
    /// File descriptor
    file: File,

    /// Path to the file
    path: PathBuf,

    /// Base offset of the log
    base_offset: u64,

    /// current file position for the write
    write_pos: usize,

    /// Maximum number of bytes permitted to be appended to the log
    max_bytes: usize,
}

impl Segment {
    pub fn new<P>(log_dir: P, base_offset: u64, max_bytes: usize) -> io::Result<Segment>
    where
        P: AsRef<Path>,
    {
        let log_path = {
            // the log is of the form BASE_OFFSET.log
            let mut path_buf = PathBuf::new();
            path_buf.push(&log_dir);
            path_buf.push(format!("{:020}", base_offset));
            path_buf.set_extension(SEGMENT_FILE_NAME_EXTENSION);
            path_buf
        };

        let mut f = OpenOptions::new().read(true).create_new(true).append(true).open(&log_path)?;

        // add the magic
        f.write_all(&VERSION_1_MAGIC)?;

        Ok(Segment { file: f, path: log_path, base_offset, write_pos: 2, max_bytes })
    }

    pub fn open<P>(seg_path: P, max_bytes: usize) -> io::Result<Segment>
    where
        P: AsRef<Path>,
    {
        let seg_file = OpenOptions::new().read(true).write(true).append(true).open(&seg_path)?;

        let filename = seg_path.as_ref().file_name().unwrap().to_str().unwrap();
        let base_offset = match u64::from_str_radix(&filename[0..SEGMENT_FILE_NAME_LEN], 10) {
            Ok(v) => v,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Segment file name does not parse as u64",
                ));
            }
        };

        let meta = seg_file.metadata()?;

        // check the magic
        {
            let mut bytes = [0u8; 2];
            let size = seg_file.read_at(&mut bytes, 0)?;
            if size < 2 || bytes != VERSION_1_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Segment file {} does not contain Version 1 \
                         magic",
                        filename
                    ),
                ));
            }
        }

        info!("Opened segment {}", filename);

        Ok(Segment {
            file: seg_file,
            path: seg_path.as_ref().to_path_buf(),
            write_pos: meta.len() as usize,
            base_offset,
            max_bytes,
        })
    }

    pub fn size(&self) -> usize {
        self.write_pos
    }

    #[inline]
    pub fn starting_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn append<T: MessageSet>(
        &mut self,
        payload: &T,
    ) -> Result<AppendMetadata, SegmentAppendError> {
        // ensure we have the capacity
        let payload_len = payload.bytes().len();
        if payload_len + self.write_pos > self.max_bytes {
            return Err(SegmentAppendError::LogFull);
        }

        let meta = AppendMetadata { starting_position: self.write_pos };

        self.file.write_all(payload.bytes())?;
        self.write_pos += payload_len;
        Ok(meta)
    }

    pub fn flush_sync(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn read_slice<T: LogSliceReader>(
        &self,
        reader: &mut T,
        file_pos: u32,
        bytes: u32,
    ) -> Result<T::Result, MessageError> {
        reader.read_from(&self.file, file_pos, bytes as usize)
    }

    /// Removes the segment file.
    pub fn remove(self) -> io::Result<()> {
        let path = self.path.clone();
        drop(self);

        info!("Removing segment file {}", path.display());
        fs::remove_file(path)
    }

    /// Truncates the segment file to desired length. Other methods should
    /// ensure that the truncation is at the message boundary.
    pub fn truncate(&mut self, length: u32) -> io::Result<()> {
        self.file.set_len(u64::from(length))?;
        self.write_pos = length as usize;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{message::set_offsets, testutil::*},
        *,
    };
    use std::{fs, path::PathBuf};

    #[test]
    pub fn log_append() {
        let path = TestDir::new();
        let mut f = Segment::new(path, 0, 1024).unwrap();

        {
            let mut buf = MessageBuf::default();
            buf.push("12345").unwrap();
            let meta = f.append(&mut buf).unwrap();
            assert_eq!(2, meta.starting_position);
        }

        {
            let mut buf = MessageBuf::default();
            buf.push("66666").unwrap();
            buf.push("77777").unwrap();
            let meta = f.append(&mut buf).unwrap();
            assert_eq!(27, meta.starting_position);

            let mut it = buf.iter();
            let p0 = it.next().unwrap();
            assert_eq!(p0.total_bytes(), 25);

            let p1 = it.next().unwrap();
            assert_eq!(p1.total_bytes(), 25);
        }

        f.flush_sync().unwrap();
    }

    #[test]
    pub fn log_open() {
        let log_dir = TestDir::new();

        {
            let mut f = Segment::new(&log_dir, 0, 1024).unwrap();
            let mut buf = MessageBuf::default();
            buf.push("12345").unwrap();
            buf.push("66666").unwrap();
            f.append(&mut buf).unwrap();
            f.flush_sync().unwrap();
        }

        // open it
        {
            let mut path_buf = PathBuf::new();
            path_buf.push(&log_dir);
            path_buf.push(format!("{:020}", 0));
            path_buf.set_extension(SEGMENT_FILE_NAME_EXTENSION);

            let res = Segment::open(&path_buf, 1024);
            assert!(res.is_ok(), "Err {:?}", res.err());

            let f = res.unwrap();
            assert_eq!(0, f.starting_offset());
        }
    }

    #[test]
    pub fn log_read() {
        let log_dir = TestDir::new();
        let mut f = Segment::new(&log_dir, 0, 1024).unwrap();

        {
            let mut buf = MessageBuf::default();
            buf.push("0123456789").unwrap();
            buf.push("aaaaaaaaaa").unwrap();
            buf.push("abc").unwrap();
            set_offsets(&mut buf, 0);
            f.append(&mut buf).unwrap();
        }

        let mut reader = MessageBufReader;
        let msgs = f.read_slice(&mut reader, 2, 83).unwrap();
        assert_eq!(3, msgs.len());

        for (i, m) in msgs.iter().enumerate() {
            assert_eq!(i as u64, m.offset());
        }
    }

    #[test]
    pub fn log_read_with_size_limit() {
        let log_dir = TestDir::new();
        let mut f = Segment::new(&log_dir, 0, 1024).unwrap();

        let mut buf = MessageBuf::default();
        buf.push("0123456789").unwrap();
        buf.push("aaaaaaaaaa").unwrap();
        buf.push("abc").unwrap();
        set_offsets(&mut buf, 0);
        let meta = f.append(&mut buf).unwrap();

        let second_msg_start = {
            let mut it = buf.iter();
            let mut pos = meta.starting_position;
            pos += it.next().unwrap().total_bytes();
            pos
        };

        // byte max contains message 0
        let mut reader = MessageBufReader;
        let msgs = f.read_slice(&mut reader, 2, second_msg_start as u32 - 2).unwrap();

        assert_eq!(1, msgs.len());
    }

    #[test]
    pub fn log_read_from_write() {
        let log_dir = TestDir::new();
        let mut f = Segment::new(&log_dir, 0, 1024).unwrap();

        {
            let mut buf = MessageBuf::default();
            buf.push("0123456789").unwrap();
            buf.push("aaaaaaaaaa").unwrap();
            buf.push("abc").unwrap();
            set_offsets(&mut buf, 0);
            f.append(&mut buf).unwrap();
        }

        let mut reader = MessageBufReader;
        let msgs = f.read_slice(&mut reader, 2, 83).unwrap();
        assert_eq!(3, msgs.len());

        {
            let mut buf = MessageBuf::default();
            buf.push("foo").unwrap();
            set_offsets(&mut buf, 3);
            f.append(&mut buf).unwrap();
        }

        let msgs = f.read_slice(&mut reader, 2, 106).unwrap();
        assert_eq!(4, msgs.len());

        for (i, m) in msgs.iter().enumerate() {
            assert_eq!(i as u64, m.offset());
        }
    }

    #[test]
    pub fn log_remove() {
        let log_dir = TestDir::new();
        let f = Segment::new(&log_dir, 0, 1024).unwrap();

        let seg_exists = fs::read_dir(&log_dir)
            .unwrap()
            .find(|entry| {
                let path = entry.as_ref().unwrap().path();
                path.file_name().unwrap() == "00000000000000000000.log"
            })
            .is_some();
        assert!(seg_exists, "Segment file does not exist?");

        f.remove().unwrap();

        let seg_exists = fs::read_dir(&log_dir)
            .unwrap()
            .find(|entry| {
                let path = entry.as_ref().unwrap().path();
                path.file_name().unwrap() == "00000000000000000000.log"
            })
            .is_some();
        assert!(!seg_exists, "Segment file should have been removed");
    }

    #[test]
    pub fn log_truncate() {
        let log_dir = TestDir::new();
        let mut f = Segment::new(&log_dir, 0, 1024).unwrap();

        let mut buf = MessageBuf::default();
        buf.push("0123456789").unwrap();
        buf.push("aaaaaaaaaa").unwrap();
        buf.push("abc").unwrap();
        set_offsets(&mut buf, 0);
        let meta = f.append(&mut buf).unwrap();

        let mut reader = MessageBufReader;
        let msg_buf = f
            .read_slice(&mut reader, 2, f.size() as u32 - 2)
            .expect("Read after first append failed");
        assert_eq!(3, msg_buf.len());

        // find the second message starting point position in the segment
        let second_msg_start = {
            let mut it = buf.iter();
            let mut pos = meta.starting_position;
            pos += it.next().unwrap().total_bytes();
            pos
        };

        // truncate to first message
        f.truncate(second_msg_start as u32).unwrap();

        assert_eq!(second_msg_start, f.size());

        let size = fs::metadata(&f.path).unwrap().len();
        assert_eq!(second_msg_start as u64, size);

        let meta2 = {
            let mut buf = MessageBuf::default();
            buf.push("zzzzzzzzzz").unwrap();
            set_offsets(&mut buf, 1);
            f.append(&mut buf).unwrap()
        };
        assert_eq!(second_msg_start, meta2.starting_position);

        let size = fs::metadata(&f.path).unwrap().len();
        assert_eq!(f.size() as u64, size);

        // read the log
        let mut reader = MessageBufReader;
        let msg_buf = f
            .read_slice(&mut reader, 2, f.size() as u32 - 2)
            .expect("Read after second append failed");
        assert_eq!(2, msg_buf.len());
    }
}
