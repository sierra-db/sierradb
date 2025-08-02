// Generate implementation for function skeleton, we use this for
// `AsyncTypedCommands` because we want to be able to handle having a return
// type specified or unspecified with a fallback
#[cfg(feature = "aio")]
macro_rules! implement_command_async {
	// If the return type is `Generic`, then we require the user to specify the return type
	(
        $lifetime: lifetime
		$(#[$attr:meta])+
		fn $name:ident<$($tyargs:ident : $ty:ident),*>(
			$($argname:ident: $argty:ty),*) $body:block Generic
    ) => {
		$(#[$attr])*
		#[inline]
		#[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
		fn $name<$lifetime, RV: FromRedisValue, $($tyargs: $ty + Send + Sync + $lifetime,)*>(
			& $lifetime mut self
			$(, $argname: $argty)*
		) -> redis::RedisFuture<$lifetime, RV>
		{
			Box::pin(async move { ($body).query_async(self).await })
		}
	};

	// If return type is specified in the input skeleton, then we will return it in the generated function (note match rule `$rettype:ty`)
	(
        $lifetime: lifetime
		$(#[$attr:meta])+
		fn $name:ident<$($tyargs:ident : $ty:ident),*>(
			$($argname:ident: $argty:ty),*) $body:block $rettype:ty
    ) => {
		$(#[$attr])*
		#[inline]
		#[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
		fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)*>(
			& $lifetime mut self
			$(, $argname: $argty)*
		) -> redis::RedisFuture<$lifetime, $rettype>

		{
			Box::pin(async move { ($body).query_async(self).await })
		}
	};
}

macro_rules! implement_command_sync {
	// If the return type is `Generic`, then we require the user to specify the return type
	(
        $lifetime: lifetime
		$(#[$attr:meta])+
		fn $name:ident<$($tyargs:ident : $ty:ident),*>(
			$($argname:ident: $argty:ty),*) $body:block Generic
    ) => {
		$(#[$attr])*
		#[inline]
		#[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
		fn $name<$lifetime, RV: FromRedisValue, $($tyargs: $ty + Send + Sync + $lifetime,)*>(
			& $lifetime mut self
			$(, $argname: $argty)*
		) -> RedisResult<RV>
		{
			Cmd::$name($($argname),*).query(self)
		}
	};

	// If return type is specified in the input skeleton, then we will return it in the generated function (note match rule `$rettype:ty`)
	(
        $lifetime: lifetime
		$(#[$attr:meta])+
		fn $name:ident<$($tyargs:ident : $ty:ident),*>(
			$($argname:ident: $argty:ty),*) $body:block $rettype:ty
    ) => {
		$(#[$attr])*
		#[inline]
		#[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
		fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)*>(
			& $lifetime mut self
			$(, $argname: $argty)*
		) -> redis::RedisResult<$rettype>

		{
			redis::Cmd::$name($($argname),*).query(self)
		}
	};
}

macro_rules! implement_commands {
    (
        $lifetime: lifetime
        $(
            $(#[$attr:meta])+
            fn $name:ident<$($tyargs:ident : $ty:ident),*>(
                $($argname:ident: $argty:ty),*) -> $rettype:tt $body:block
        )*
    ) =>
    (
        /// Implements common redis commands for connection like objects.
        ///
        /// This allows you to send commands straight to a connection or client.
        /// It is also implemented for redis results of clients which makes for
        /// very convenient access in some basic cases.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// # fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_connection()?;
        /// redis::cmd("SET").arg("my_key").arg(42).exec(&mut con).unwrap();
        /// assert_eq!(redis::cmd("GET").arg("my_key").query(&mut con), Ok(42));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// # fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_connection()?;
        /// let _: () = con.set("my_key", 42)?;
        /// assert_eq!(con.get("my_key"), Ok(42));
        /// # Ok(()) }
        /// ```
        pub trait Commands : redis::ConnectionLike + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty, )*>(
                    &mut self $(, $argname: $argty)*) -> redis::RedisResult<$rettype>
                    { redis::Cmd::$name($($argname),*).query(self) }
            )*
        }

        pub trait CmdExt {
            $(
                $(#[$attr])*
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty),*>($($argname: $argty),*) -> Self;
            )*
        }

        impl CmdExt for redis::Cmd {
            $(
                $(#[$attr])*
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty),*>($($argname: $argty),*) -> Self {
                    ::std::mem::take($body)
                }
            )*
        }

        /// Implements common redis commands over asynchronous connections.
        ///
        /// This allows you to send commands straight to a connection or client.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// use redis::AsyncCommands;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_multiplexed_async_connection().await?;
        /// redis::cmd("SET").arg("my_key").arg(42i32).exec_async(&mut con).await?;
        /// assert_eq!(redis::cmd("GET").arg("my_key").query_async(&mut con).await, Ok(42i32));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// use redis::AsyncCommands;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_multiplexed_async_connection().await?;
        /// let _: () = con.set("my_key", 42i32).await?;
        /// assert_eq!(con.get("my_key").await, Ok(42i32));
        /// # Ok(()) }
        /// ```
        #[cfg(feature = "aio")]
        pub trait AsyncCommands : redis::aio::ConnectionLike + Send + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)* RV>(
                    & $lifetime mut self
                    $(, $argname: $argty)*
                ) -> redis::RedisFuture<$lifetime, RV>
                where
                    RV: redis::FromRedisValue,
                {
                    Box::pin(async move { ($body).query_async(self).await })
                }
            )*
        }

        /// Implements common redis commands.
        /// The return types are concrete and opinionated. If you want to choose the return type you should use the `Commands` trait.
        pub trait TypedCommands: redis::ConnectionLike + Sized {
            $(
				implement_command_sync! {
					$lifetime
					$(#[$attr])*
					fn $name<$($tyargs: $ty),*>(
						$($argname: $argty),*
					)

					{
						$body
					} $rettype
				}
            )*
        }

		/// Implements common redis commands over asynchronous connections.
        /// The return types are concrete and opinionated. If you want to choose the return type you should use the `AsyncCommands` trait.
		#[cfg(feature = "aio")]
        pub trait AsyncTypedCommands: redis::aio::ConnectionLike + Send + Sized {
            $(
				implement_command_async! {
					$lifetime
					$(#[$attr])*
					fn $name<$($tyargs: $ty),*>(
						$($argname: $argty),*
					)

					{
						$body
					} $rettype
				}
            )*
        }

        /// Implements common redis commands for pipelines.  Unlike the regular
        /// commands trait, this returns the pipeline rather than a result
        /// directly.  Other than that it works the same however.
        pub trait PipelineExt {
            $(
                $(#[$attr])*
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self;
            )*
        }

        impl PipelineExt for redis::Pipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self {
                    self.add_command(::std::mem::take($body))
                }
            )*
        }

        // Implements common redis commands for cluster pipelines.  Unlike the regular
        // commands trait, this returns the cluster pipeline rather than a result
        // directly.  Other than that it works the same however.
        #[cfg(feature = "cluster")]
        pub trait ClusterPipeline {
            $(
                $(#[$attr])*
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self;
            )*

        }

        #[cfg(feature = "cluster")]
        impl ClusterPipeline for redis::cluster::ClusterPipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self {
                    self.add_command(::std::mem::take($body))
                }
            )*
        }
    );
}
