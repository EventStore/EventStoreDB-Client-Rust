use eventstore_macros::{options, streaming};

options! {
    #[derive(Clone, Default)]
    #[streaming]
    pub struct BatchAppendOptions {}
}
