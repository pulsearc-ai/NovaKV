use std::error::Error as StdError;
use std::fmt;

pub type Result<T> = std::result::Result<T, Status>;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Code {
    Ok,
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    IoError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    code: Code,
    message: String,
}

impl Status {
    pub fn ok() -> Self {
        Self {
            code: Code::Ok,
            message: String::new(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(Code::NotFound, message)
    }

    pub fn corruption(message: impl Into<String>) -> Self {
        Self::new(Code::Corruption, message)
    }

    pub fn not_supported(message: impl Into<String>) -> Self {
        Self::new(Code::NotSupported, message)
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    pub fn io_error(message: impl Into<String>) -> Self {
        Self::new(Code::IoError, message)
    }

    pub fn with_context(
        code: Code,
        message: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        let message = message.into();
        let context = context.into();
        if context.is_empty() {
            Self::new(code, message)
        } else {
            Self::new(code, format!("{message}: {context}"))
        }
    }

    pub fn code(&self) -> Code {
        self.code
    }
    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_ok(&self) -> bool {
        self.code == Code::Ok
    }
    pub fn is_not_found(&self) -> bool {
        self.code == Code::NotFound
    }
    pub fn is_corruption(&self) -> bool {
        self.code == Code::Corruption
    }
    pub fn is_not_supported(&self) -> bool {
        self.code == Code::NotSupported
    }
    pub fn is_invalid_argument(&self) -> bool {
        self.code == Code::InvalidArgument
    }
    pub fn is_io_error(&self) -> bool {
        self.code == Code::IoError
    }

    fn new(code: Code, message: impl Into<String>) -> Self {
        debug_assert_ne!(code, Code::Ok);
        Self {
            code,
            message: message.into(),
        }
    }
}

impl Default for Status {
    fn default() -> Self {
        Self::ok()
    }
}

impl fmt::Display for Status {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.code {
            Code::Ok => formatter.write_str("OK"),
            Code::NotFound => write!(formatter, "NotFound: {}", self.message),
            Code::Corruption => write!(formatter, "Corruption: {}", self.message),
            Code::NotSupported => write!(formatter, "Not implemented: {}", self.message),
            Code::InvalidArgument => write!(formatter, "Invalid argument: {}", self.message),
            Code::IoError => write!(formatter, "IO error: {}", self.message),
        }
    }
}

impl StdError for Status {}
