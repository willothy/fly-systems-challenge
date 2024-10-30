use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error<E: std::error::Error + Sized + 'static> {
    #[snafu(display("IO error: {}", source))]
    Io {
        #[snafu(source)]
        source: std::io::Error,
    },

    #[snafu(display("Node error: {}", source))]
    Node {
        #[snafu(source)]
        source: E,
    },

    #[snafu(display("Node error: {}", source))]
    Internal {
        #[snafu(source)]
        source: crate::node::InternalError,
    },

    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },
}

impl<E: std::error::Error> From<std::io::Error> for Error<E> {
    fn from(source: std::io::Error) -> Self {
        Self::Io { source }
    }
}

pub type Result<T, E = Box<dyn std::error::Error + 'static>> = std::result::Result<T, Error<E>>;
