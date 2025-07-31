#[macro_export]
macro_rules! impl_command {
    ($cmd:ident, [ $( $req_field:ident ),* ], [ $( $opt_field:ident ),* ]) => {
        impl FromArgs for $cmd {
            fn from_args(args: &[Value]) -> Result<Self, Value> {
                let mut iter = args.into_iter();

                $(
                    let $req_field = TryFrom::try_from(iter.next().ok_or_else(|| Value::Error(
                        concat!(
                            "Missing ",
                            stringify!($req_field),
                            " argument"
                        ).to_string()
                    ))?).map_err(|err: io::Error| Value::Error(err.to_string()))?;
                )*

                $(
                    let mut $opt_field = None;
                )*

                loop {
                    let Some(name) = iter.next() else {
                        break;
                    };
                    let name = name.as_str().map_err(|_| Value::Error("Expected field name".to_string()))?;
                    #[allow(unused_variables)]
                    let value = iter.next().ok_or_else(|| Value::Error("Unexpected value".to_string()))?;

                    match name.to_lowercase().as_str() {
                        $(
                            stringify!($opt_field) => {
                                $opt_field = Some(TryFrom::try_from(value).map_err(|err: io::Error| Value::Error(err.to_string()))?);
                            }
                        )*
                        _ => return Err(Value::Error(format!("Unknown field {name}")))
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
