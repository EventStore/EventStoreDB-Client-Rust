use eventstore_macros::options;

options! {
    #[derive(Clone, Default)]
    pub struct CreateProjectionOptions {
        pub(crate) track_emitted_streams: bool,
        pub(crate) emit: bool,
    }
}

impl CreateProjectionOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn track_emitted_streams(self, track_emitted_streams: bool) -> Self {
        Self {
            track_emitted_streams,
            ..self
        }
    }

    pub fn emit(self, emit: bool) -> Self {
        Self { emit, ..self }
    }
}

options! {
    #[derive(Clone, Default)]
    pub struct UpdateProjectionOptions {
        pub(crate) emit: Option<bool>,
    }
}

impl UpdateProjectionOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn emit(self, emit: bool) -> Self {
        Self {
            emit: Some(emit),
            ..self
        }
    }
}

options! {
    #[derive(Clone, Default)]
    pub struct DeleteProjectionOptions {
        pub(crate) delete_emitted_streams: bool,
        pub(crate) delete_state_stream: bool,
        pub(crate) delete_checkpoint_stream: bool,
    }
}

impl DeleteProjectionOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn delete_emitted_streams(self, delete_emitted_streams: bool) -> Self {
        Self {
            delete_emitted_streams,
            ..self
        }
    }

    pub fn delete_state_stream(self, delete_state_stream: bool) -> Self {
        Self {
            delete_state_stream,
            ..self
        }
    }

    pub fn delete_checkpoint_stream(self, delete_checkpoint_stream: bool) -> Self {
        Self {
            delete_checkpoint_stream,
            ..self
        }
    }
}

options! {
    #[derive(Clone, Default)]
    pub struct GetStateProjectionOptions {
        pub(crate) partition: String,
    }
}

impl GetStateProjectionOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn partition(self, value: impl AsRef<str>) -> Self {
        Self {
            partition: value.as_ref().to_string(),
            ..self
        }
    }
}

options! {
    #[derive(Clone, Default)]
    pub struct GetResultProjectionOptions {
        pub(crate) partition: String,
    }
}

impl GetResultProjectionOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn partition(self, value: impl AsRef<str>) -> Self {
        Self {
            partition: value.as_ref().to_string(),
            ..self
        }
    }
}

options! {
    #[derive(Clone, Default)]
    pub struct GenericProjectionOptions {}
}
