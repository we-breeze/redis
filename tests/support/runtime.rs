//! Keep the process-wide DNS resolver alive across concurrent test cases.

pub fn run(future: impl std::future::Future<Output = ()>) {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
        })
        .block_on(future);
}

macro_rules! runtime_test {
    (async fn $name:ident() $body:block) => {
        #[test]
        fn $name() {
            runtime::run(async $body);
        }
    };
}
pub(crate) use runtime_test;
