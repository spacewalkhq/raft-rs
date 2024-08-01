use chrono::prelude::*;
use slog::{o, Drain, Logger};

pub fn get_logger() -> Logger {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator)
        .use_custom_timestamp(|io| write!(io, "{}", Utc::now().format("%Y-%m-%d %H:%M:%S")))
        .build()
        .fuse();

    Logger::root(drain, o!())
}

#[cfg(test)]
mod tests {
    use slog::{crit, debug, error, info, trace, warn};

    use crate::log::get_logger;

    #[tokio::test]
    async fn test_slog() {
        let log = get_logger();

        trace!(log, "trace log message");
        debug!(log, "debug log message");
        info!(log, "info log message");
        warn!(log, "warn log message");
        error!(log, "error log message");
        crit!(log, "crit log message");
    }
}
