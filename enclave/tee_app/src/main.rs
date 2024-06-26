mod crypto;
mod graph;
mod server;

use anyhow::Result;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    logger::init();

    server::start_server().await?;

    Ok(())
}

mod logger {
    use log::{Level, LevelFilter, Metadata, Record};

    pub fn init() {
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Trace))
            .expect("failed to init the logger");
    }

    static LOGGER: SimpleLogger = SimpleLogger;

    struct SimpleLogger;

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() <= Level::Info
        }

        fn log(&self, record: &Record) {
            if self.enabled(record.metadata()) {
                println!("[{}] {}", record.level(), record.args());
            }
        }

        fn flush(&self) {}
    }
}
