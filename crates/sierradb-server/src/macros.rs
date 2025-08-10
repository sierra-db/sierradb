#[macro_export]
macro_rules! impl_command {
    ($cmd:ident, [ $( $req_field:ident ),* ], [ $( $opt_field:ident ),* ]) => {
        impl $crate::request::FromArgs for $cmd {
            fn from_args(args: &[redis_protocol::resp3::types::BytesFrame]) -> Result<Self, String> {
                let mut iter = args.into_iter();

                $(
                    let $req_field = $crate::request::FromBytesFrame::from_bytes_frame(
                            iter.next().ok_or_else(|| {
                                sierradb_protocol::error::ErrorCode::InvalidArg
                                    .with_message(
                                        concat!("missing ", stringify!($req_field), " argument")
                                    )
                            })?
                        )
                        .map_err(|err| {
                            sierradb_protocol::error::ErrorCode::InvalidArg
                                .with_message(
                                    format!(
                                        "{}: {err}",
                                        concat!("failed to parse ", stringify!($req_field), " argument"),
                                    )
                                )
                        })?;
                )*

                $(
                    let mut $opt_field = None;
                )*

                loop {
                    let Some(name) = iter.next() else {
                        break;
                    };
                    let name = <&str as $crate::request::FromBytesFrame>::from_bytes_frame(name)?;
                    #[allow(unused_variables)]
                    let value = iter.next().ok_or_else(|| {
                        sierradb_protocol::error::ErrorCode::InvalidArg
                            .with_message(format!("expected value after {name}"))
                    })?;

                    match name.to_lowercase().as_str() {
                        $(
                            stringify!($opt_field) => {
                                $opt_field = Some(
                                    $crate::request::FromBytesFrame::from_bytes_frame(value)
                                        .map_err(|err| {
                                            sierradb_protocol::error::ErrorCode::InvalidArg
                                                .with_message(format!("failed to parse {name} arg: {err}"))
                                        })?
                                );
                            }
                        )*
                        _ => return Err(
                            sierradb_protocol::error::ErrorCode::InvalidArg
                                .with_message(format!("unknown field '{name}'"))
                        )
                    }
                }

                Ok($cmd {
                    $( $req_field, )*
                    $( $opt_field: $opt_field.unwrap_or_default(), )*
                })
            }
        }
    };
}
