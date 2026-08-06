//! The `implement_commands!` macro.
//!
//! A single declarative list of commands expands into three things at once:
//! the default methods of the [`Commands`](crate::commands::Commands) trait
//! (async, generic over the return type), and the matching builder methods on
//! [`Pipeline`](crate::pipeline::Pipeline). Defining a command once keeps the
//! connection API and the pipeline API in lock-step.
//!
//! Grammar per entry:
//!
//! ```ignore
//! /// doc line
//! fn method_name(arg1, arg2, ...) => ["VERB", "SUBVERB"];
//! fn method_name(arg1) => ["VERB"] => ["SUFFIX"];   // trailing literals
//! ```
//!
//! Every argument is `impl ToRedisArgs`, so scalars, slices, tuples and maps
//! all work. The first bracketed literals are the fixed command words written
//! before the arguments; an optional second bracket appends fixed literals
//! after the arguments (e.g. `WITHSCORES`).

/// See the [module docs](self).
#[macro_export]
macro_rules! implement_commands {
    (
        $(
            $(#[doc = $doc:literal])*
            fn $name:ident ( $($arg:ident),* $(,)? ) => [ $($verb:literal),+ ]
                $(=> [ $($suffix:literal),+ ])? ;
        )*
    ) => {
        /// Typed, async Redis command methods.
        ///
        /// Implemented for every [`ConnectionLike`](crate::connection::ConnectionLike),
        /// so it is available on raw connections, pooled clients, and the HA
        /// wrappers alike. Each method builds a command and returns a future
        /// yielding a value converted via [`FromRedisValue`](crate::from_value::FromRedisValue).
        pub trait Commands: $crate::connection::ConnectionLike {
            $(
                $(#[doc = $doc])*
                fn $name<'a, RV: $crate::from_value::FromRedisValue>(
                    &'a self,
                    $($arg: impl $crate::to_args::ToRedisArgs),*
                ) -> $crate::connection::RedisFuture<'a, RV> {
                    let mut command = $crate::cmd::Cmd::new();
                    $( command.arg($verb); )+
                    $( command.arg($arg); )*
                    $( $( command.arg($suffix); )+ )?
                    Box::pin(async move { command.query_async(self).await })
                }
            )*
        }

        impl<T: $crate::connection::ConnectionLike + ?Sized> Commands for T {}

        impl $crate::pipeline::Pipeline {
            $(
                $(#[doc = $doc])*
                pub fn $name(
                    &mut self,
                    $($arg: impl $crate::to_args::ToRedisArgs),*
                ) -> &mut Self {
                    let mut command = $crate::cmd::Cmd::new();
                    $( command.arg($verb); )+
                    $( command.arg($arg); )*
                    $( $( command.arg($suffix); )+ )?
                    self.add_command(command)
                }
            )*
        }
    };
}
