use kurrent::EventData;
use serde_json::json;

pub fn fresh_stream_id(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4();

    format!("{}-{}", prefix, uuid)
}

pub fn generate_events<Type: AsRef<str>>(event_type: Type, cnt: usize) -> Vec<EventData> {
    let mut events = Vec::with_capacity(cnt);

    for idx in 1..cnt + 1 {
        let payload = json!({
            "event_index": idx,
        });

        let data = EventData::json(event_type.as_ref(), &payload).unwrap();
        events.push(data);
    }

    events
}
