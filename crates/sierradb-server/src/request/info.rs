use combine::{Parser, choice};
use indexmap::indexmap;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::cache::SegmentBlockCache;

use crate::parser::{FrameStream, keyword};
use crate::request::{HandleRequest, blob_str, map, number};
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

    async fn handle_request(self, _conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let resp = match self.section {
            InfoSection::Cache => {
                let hits = SegmentBlockCache::cache_hits();
                let misses = SegmentBlockCache::cache_misses();
                let blocks_evicted = SegmentBlockCache::blocks_evicted();
                InfoResp::Cache(InfoCacheResp {
                    hits,
                    misses,
                    blocks_evicted,
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
    blocks_evicted: u64,
}

impl From<InfoCacheResp> for BytesFrame {
    fn from(resp: InfoCacheResp) -> Self {
        map(indexmap! {
            blob_str("hits") => number(resp.hits as i64),
            blob_str("misses") => number(resp.misses as i64),
            blob_str("blocks_evicted") => number(resp.blocks_evicted as i64),
        })
    }
}
