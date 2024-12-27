#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// A command retry policy.
pub struct RetryOptions {
    pub(crate) limit: usize,
    pub(crate) delay: std::time::Duration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            limit: 3,
            delay: std::time::Duration::from_millis(500),
        }
    }
}

impl RetryOptions {
    /// Sets how many time we retry a failing command before giving up.
    pub fn retry_limit(self, limit: usize) -> Self {
        Self { limit, ..self }
    }

    /// Keep retrying regardless of how many times we failed.
    pub fn retry_forever(self) -> Self {
        self.retry_limit(usize::MAX)
    }

    /// When a command failed, sets how long we wait before retrying.
    pub fn retry_delay(self, delay: std::time::Duration) -> Self {
        Self { delay, ..self }
    }
}
