use std::{net::SocketAddr, path::PathBuf, sync::OnceLock, time::Duration};

use clap::{
    builder::{TypedValueParser, ValueParser},
    error::{ContextKind, ContextValue, ErrorKind},
    Arg, Command, Error, Parser,
};
use duration_human::DurationHuman;

pub static ARGS: OnceLock<Args> = OnceLock::new();

pub fn init_args() -> &'static Args {
    ARGS.set(Args::parse()).unwrap();
    ARGS.get().unwrap()
}

/// Event store
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to database directory
    #[arg(short, long, default_value = ".log")]
    pub path: PathBuf,

    /// GRPC address to listen on
    #[arg(short, long, env, default_value = "[::1]:9220")]
    pub addr: SocketAddr,

    /// How frequently events should be flushed to disk
    #[arg(long, env, default_value = "100ms", value_parser = ValueParser::new(HumanReadableValueParser))]
    pub flush_interval: Duration,
}

#[derive(Clone)]
struct HumanReadableValueParser;

impl TypedValueParser for HumanReadableValueParser {
    type Value = Duration;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, Error> {
        let id = arg.unwrap().get_id();
        let s = value.to_str().ok_or_else(|| {
            let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
            err.insert(
                ContextKind::InvalidArg,
                ContextValue::String(id.to_string()),
            );
            err.insert(ContextKind::InvalidValue, ContextValue::None);
            err
        })?;
        let duration = DurationHuman::parse(s).map_err(|e| {
            let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
            err.insert(
                ContextKind::InvalidArg,
                ContextValue::String(id.to_string()),
            );
            err.insert(
                ContextKind::InvalidValue,
                ContextValue::String(s.to_string()),
            );
            err.insert(ContextKind::Usage, ContextValue::String(e.to_string()));
            err
        })?;

        Ok((&duration).into())
    }
}
