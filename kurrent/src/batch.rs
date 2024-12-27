use crate::{EventData, ExpectedRevision, Position};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};

#[derive(Debug)]
pub(crate) struct In {
    req: Req,
    sender: oneshot::Sender<crate::Result<BatchWriteResult>>,
}

#[derive(Debug)]
pub(crate) struct Req {
    pub(crate) id: uuid::Uuid,
    pub(crate) stream_name: String,
    pub(crate) events: Vec<EventData>,
    pub(crate) expected_revision: ExpectedRevision,
}

#[derive(Debug)]
pub(crate) struct Out {
    pub(crate) correlation_id: uuid::Uuid,
    pub(crate) result: crate::Result<BatchWriteResult>,
}

#[derive(Debug)]
pub(crate) enum BatchMsg {
    In(In),
    Out(Out),
    Error(crate::Error),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchWriteResult {
    stream_name: String,
    current_revision: Option<u64>,
    current_position: Option<Position>,
    expected_revision: Option<ExpectedRevision>,
}

impl BatchWriteResult {
    pub fn new(
        stream_name: String,
        current_revision: Option<u64>,
        current_position: Option<Position>,
        expected_revision: Option<ExpectedRevision>,
    ) -> Self {
        Self {
            stream_name,
            current_position,
            current_revision,
            expected_revision,
        }
    }

    pub fn stream_name(&self) -> &str {
        self.stream_name.as_str()
    }

    pub fn current_revision(&self) -> Option<u64> {
        self.current_revision
    }

    pub fn current_position(&self) -> Option<Position> {
        self.current_position
    }

    pub fn expected_version(&self) -> Option<ExpectedRevision> {
        self.expected_revision
    }
}

pub struct BatchAppendClient {
    sender: UnboundedSender<BatchMsg>,
}

impl BatchAppendClient {
    pub(crate) fn new(
        sender: UnboundedSender<BatchMsg>,
        mut receiver: UnboundedReceiver<BatchMsg>,
        forward: UnboundedSender<Req>,
    ) -> Self {
        tokio::spawn(async move {
            let mut reg = std::collections::HashMap::<
                uuid::Uuid,
                oneshot::Sender<crate::Result<BatchWriteResult>>,
            >::new();
            while let Some(msg) = receiver.recv().await {
                match msg {
                    BatchMsg::In(msg) => {
                        let correlation_id = msg.req.id;
                        if forward.send(msg.req).is_ok() {
                            reg.insert(correlation_id, msg.sender);
                            debug!("Send batch-append request {}", correlation_id);

                            continue;
                        }

                        error!("Batch-append session has been closed");
                        break;
                    }

                    BatchMsg::Out(resp) => {
                        if let Some(entry) = reg.remove(&resp.correlation_id) {
                            let failed = resp.result.is_err();
                            let _ = entry.send(resp.result);

                            if failed {
                                break;
                            }

                            continue;
                        }

                        warn!(
                            "Unknown batch-append response correlation id: {}",
                            resp.correlation_id
                        );
                    }

                    BatchMsg::Error(e) => {
                        for (_, resp_sender) in reg {
                            let _ = resp_sender.send(Err(e.clone()));
                        }

                        break;
                    }
                }
            }
        });

        Self { sender }
    }

    pub async fn append_to_stream<S: AsRef<str>>(
        &self,
        stream_name: S,
        expected_revision: ExpectedRevision,
        events: Vec<EventData>,
    ) -> crate::Result<BatchWriteResult> {
        let (sender, receiver) = oneshot::channel();
        let req = Req {
            id: uuid::Uuid::new_v4(),
            stream_name: stream_name.as_ref().to_string(),
            events,
            expected_revision,
        };

        let req = In { sender, req };

        if let Err(e) = self.sender.send(BatchMsg::In(req)) {
            error!("[sending-end] Batch-append stream is closed: {}", e);

            let status = tonic::Status::cancelled("Batch-append stream has been closed");
            return Err(crate::Error::ServerError(status.to_string()));
        }

        match receiver.await {
            Err(e) => {
                error!("[receiving-end] Batch-append stream is closed: {}", e);

                let status = tonic::Status::cancelled("Batch-append stream has been closed");

                Err(crate::Error::ServerError(status.to_string()))
            }

            Ok(result) => result,
        }
    }
}
