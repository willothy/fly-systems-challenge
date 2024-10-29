use services::echo::EchoService;
use snafu::Report;

mod tokio_serde;

mod error;
mod message;
mod node;
mod services;

pub use error::*;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_ansi(true)
        // Output logs to stderr to conform with Maelstrom spec.
        .with_writer(std::io::stderr)
        .with_thread_names(false)
        .with_file(true)
        .init();

    if let Err(e) = node::run(EchoService).await {
        println!("{}", Report::from_error(e));
    }
}
