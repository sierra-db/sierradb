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
    #[arg(long, env = "EVENTUS_LOG_PATH", default_value = ".log")]
    pub log_path: PathBuf,

    /// GRPC address to listen on
    #[arg(long, env = "EVENTUS_LISTEN_ADDR", default_value = "[::1]:9220")]
    pub listen_addr: SocketAddr,

    /// Authentication token
    #[arg(long, env = "EVENTUS_AUTH_TOKEN")]
    pub auth_token: String,

    /// How frequently events should be flushed to disk
    #[arg(long, env = "EVENTUS_FLUSH_INTERVAL", default_value = "100ms", value_parser = ValueParser::new(HumanReadableValueParser))]
    pub flush_interval: Duration,

    /// Verbosity of logging
    ///
    /// See https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html
    #[arg(long, env = "EVENTUS_LOG", default_value = "INFO")]
    pub log: String,
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
