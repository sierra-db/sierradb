use log::{error, info, trace, warn};
use std::{
    fs, io,
    mem::{replace, swap},
};

use super::{index::*, segment::*, LogOptions, Offset};
use std::collections::BTreeMap;

pub struct FileSet {
    active: (Index, Segment),
    closed: BTreeMap<u64, (Index, Segment)>,
    opts: LogOptions,
}

impl FileSet {
    pub fn load_log(opts: LogOptions) -> io::Result<FileSet> {
        let mut segments = BTreeMap::new();
        let mut indexes = BTreeMap::new();

        let files = fs::read_dir(&opts.log_dir)?
            // ignore Err results
            .filter_map(|e| e.ok())
            // ignore directories
            .filter(|e| e.metadata().map(|m| m.is_file()).unwrap_or(false));

        for f in files {
            match f.path().extension() {
                Some(ext) if SEGMENT_FILE_NAME_EXTENSION.eq(ext) => {
                    let segment = match Segment::open(f.path(), opts.log_max_bytes) {
                        Ok(seg) => seg,
                        Err(e) => {
                            error!("Unable to open segment {:?}: {}", f.path(), e);
                            return Err(e);
                        }
                    };

                    let offset = segment.starting_offset();
                    segments.insert(offset, segment);
                }
                Some(ext) if INDEX_FILE_NAME_EXTENSION.eq(ext) => {
                    let index = match Index::open(f.path()) {
                        Ok(ind) => ind,
                        Err(e) => {
                            error!("Unable to open index {:?}: {}", f.path(), e);
                            return Err(e);
                        }
                    };

                    let offset = index.starting_offset();
                    indexes.insert(offset, index);
                    // TODO: fix missing index updates (crash before write to
                    // index)
                }
                _ => {}
            }
        }

        // pair up the index and segments (there should be an index per segment)
        let mut closed = segments
            .into_iter()
            .map(move |(i, s)| {
                match indexes.remove(&i) {
                    Some(v) => (i, (v, s)),
                    None => {
                        // TODO: create the index from the segment
                        panic!("No index found for segment starting at {}", i);
                    }
                }
            })
            .collect::<BTreeMap<u64, (Index, Segment)>>();

        // try to reuse the last index if it is not full. otherwise, open a new index
        // at the correct offset
        let last_entry = closed.keys().next_back().cloned();
        let (ind, seg) = match last_entry {
            Some(off) => {
                info!("Reusing index and segment starting at offset {}", off);
                closed.remove(&off).unwrap()
            }
            None => {
                info!("Starting new index and segment at offset 0");
                let ind = Index::new(&opts.log_dir, 0, opts.index_max_bytes)?;
                let seg = Segment::new(&opts.log_dir, 0, opts.log_max_bytes)?;
                (ind, seg)
            }
        };

        // mark all closed indexes as readonly (indexes are not opened as readonly)
        for &mut (ref mut ind, _) in closed.values_mut() {
            ind.set_readonly()?;
        }

        Ok(FileSet {
            active: (ind, seg),
            closed,
            opts,
        })
    }

    pub fn active_segment_mut(&mut self) -> &mut Segment {
        &mut self.active.1
    }

    pub fn active_index_mut(&mut self) -> &mut Index {
        &mut self.active.0
    }

    pub fn active_index(&self) -> &Index {
        &self.active.0
    }

    pub fn find(&self, offset: u64) -> &(Index, Segment) {
        let active_seg_start_off = self.active.0.starting_offset();
        if offset < active_seg_start_off {
            trace!(
                "Index is contained in the active index for offset {}",
                offset
            );
            if let Some(entry) = self.closed.range(..=offset).next_back().map(|p| p.1) {
                return entry;
            }
        }
        &self.active
    }

    pub fn roll_segment(&mut self) -> io::Result<()> {
        self.active.0.set_readonly()?;
        self.active.1.flush_sync()?;

        let next_offset = self.active.0.next_offset();

        info!("Starting new segment and index at offset {}", next_offset);

        // set the segment and index to the new active index/seg
        let mut p = {
            let seg = Segment::new(&self.opts.log_dir, next_offset, self.opts.log_max_bytes)?;
            let ind = Index::new(&self.opts.log_dir, next_offset, self.opts.index_max_bytes)?;
            (ind, seg)
        };
        swap(&mut p, &mut self.active);
        self.closed.insert(p.1.starting_offset(), p);
        Ok(())
    }

    pub fn remove_after(&mut self, offset: u64) -> Vec<(Index, Segment)> {
        if offset >= self.active.0.starting_offset() {
            return vec![];
        }

        // find the midpoint
        //
        // E.g:
        //    offset = 6
        //    [0 5 10 15] => split key 5
        //
        // midpoint  is then used as the active index/segment pair
        let split_key = match self
            .closed
            .range(..=offset)
            .next_back()
            .map(|p| p.0)
            .cloned()
        {
            Some(key) => {
                trace!("File set split key for truncation {}", key);
                key
            }
            None => {
                warn!("Split key before offset {} found", offset);
                return vec![];
            }
        };

        // split off the range of close segment/index pairs including
        // the midpoint (which will become the new active index/segment)
        let mut after = self.closed.split_off(&split_key);

        let mut active = after.remove(&split_key).unwrap();
        trace!(
            "Setting active to segment starting {}",
            active.0.starting_offset()
        );
        assert!(active.0.starting_offset() <= offset);

        swap(&mut active, &mut self.active);

        let mut pairs = after.into_iter().map(|p| p.1).collect::<Vec<_>>();
        pairs.push(active);
        pairs
    }

    pub fn remove_before(&mut self, offset: u64) -> Vec<(Index, Segment)> {
        // split such that self.closed contains [..offset), suffix=[offset,...]
        let split_point = {
            match self
                .closed
                .range(..=offset)
                .next_back()
                .map(|e| e.0)
                .cloned()
            {
                Some(off) => off,
                None => return vec![],
            }
        };

        let suffix = self.closed.split_off(&split_point);

        // put the suffix back into the closed segments
        let prefix = replace(&mut self.closed, suffix);
        prefix.into_values().collect()
    }

    pub fn log_options(&self) -> &LogOptions {
        &self.opts
    }

    /// First offset written. This may not be 0 due to removal of the start of
    /// the log
    pub fn min_offset(&self) -> Option<Offset> {
        if let Some(v) = self.closed.keys().next() {
            Some(*v)
        } else if !self.active.0.is_empty() {
            Some(self.active.0.starting_offset())
        } else {
            None
        }
    }
}
