use combine::{Parser, choice};
use indexmap::indexmap;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::cache::{BLOCK_SIZE, SegmentBlockCache};

use crate::parser::{FrameStream, keyword};
use crate::request::{HandleRequest, blob_str, double, map, number};
use crate::server::Conn;

pub struct Info {
    pub section: InfoSection,
}

impl Info {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = Info> + 'a {
        InfoSection::parser().map(|section| Info { section })
    }
}

pub enum InfoSection {
    Cache,
}

impl InfoSection {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = InfoSection> + 'a {
        choice!(keyword("CACHE").map(|_| InfoSection::Cache))
    }
}

impl HandleRequest for Info {
    type Error = String;
    type Ok = InfoResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let resp = match self.section {
            InfoSection::Cache => {
                let hits = SegmentBlockCache::cache_hits();
                let misses = SegmentBlockCache::cache_misses();
                let hit_ratio = if hits + misses > 0 {
                    let total = (hits + misses) as f64;
                    (hits as f64 / total * 1000.0).round() / 1000.0
                } else {
                    0.0
                };

                let entries: u64 = conn
                    .caches
                    .values()
                    .map(|cache| cache.cache().entry_count())
                    .sum();
                let memory_bytes: u64 = conn
                    .caches
                    .values()
                    .map(|cache| cache.cache().weighted_size())
                    .sum();
                let max_bytes = conn.cache_capacity_bytes;
                let block_size_bytes = BLOCK_SIZE;
                let evictions = SegmentBlockCache::blocks_evicted();

                InfoResp::Cache(InfoCacheResp {
                    hits,
                    misses,
                    hit_ratio,
                    entries,
                    memory_bytes,
                    max_bytes,
                    block_size_bytes,
                    evictions,
                })
            }
        };

        Ok(Some(resp))
    }
}

pub enum InfoResp {
    Cache(InfoCacheResp),
}

impl From<InfoResp> for BytesFrame {
    fn from(resp: InfoResp) -> Self {
        match resp {
            InfoResp::Cache(resp) => resp.into(),
        }
    }
}

pub struct InfoCacheResp {
    hits: u64,
    misses: u64,
    hit_ratio: f64,
    entries: u64,
    memory_bytes: u64,
    max_bytes: usize,
    block_size_bytes: usize,
    evictions: u64,
}

impl From<InfoCacheResp> for BytesFrame {
    fn from(resp: InfoCacheResp) -> Self {
        map(indexmap! {
            blob_str("hits") => number(resp.hits as i64),
            blob_str("misses") => number(resp.misses as i64),
            blob_str("hit_ratio") => double(resp.hit_ratio),
            blob_str("entries") => number(resp.entries as i64),
            blob_str("memory_bytes") => number(resp.memory_bytes as i64),
            blob_str("max_bytes") => number(resp.max_bytes as i64),
            blob_str("block_size_bytes") => number(resp.block_size_bytes as i64),
            blob_str("evictions") => number(resp.evictions as i64),
        })
    }
}
