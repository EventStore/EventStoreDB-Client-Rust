use std::fs;

pub fn generate() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = "src/event_store/generated/";
    std::fs::create_dir_all(out_dir)?;

    let files = ["protos/shared.proto"];

    // Common files.
    tonic_build::configure()
        .build_server(false)
        .extern_path(".event_store.client.Empty", "()")
        .bytes(&["StreamIdentifier.stream_name"])
        .out_dir(out_dir)
        .compile_protos(&files, &[""])?;

    for entry in std::fs::read_dir(out_dir)? {
        let file = entry.unwrap();
        let filename_string = file.file_name().into_string().expect("valid utf-8 string");

        if filename_string.starts_with("event_store.client.") {
            if filename_string.as_str() != "event_store.client.rs" {
                continue;
            }

            let new_file = file.path().parent().unwrap().join("common.rs");

            fs::rename(file.path(), new_file)?;
        }
    }

    let files = [
        "protos/persistent.proto",
        "protos/streams.proto",
        "protos/gossip.proto",
        "protos/projections.proto",
        "protos/serverfeatures.proto",
        "protos/monitoring.proto",
        "protos/operations.proto",
        "protos/users.proto",
    ];

    fs::create_dir_all(out_dir)?;

    tonic_build::configure()
        .build_server(false)
        .bytes(&[
            "AppendReq.ProposedMessage.custom_metadata",
            "AppendReq.ProposedMessage.data",
            "BatchAppendReq.ProposedMessage.custom_metadata",
            "BatchAppendReq.ProposedMessage.data",
            "ReadEvent.RecordedEvent.custom_metadata",
            "ReadEvent.RecordedEvent.data",
            "StreamIdentifier.stream_name",
        ])
        .out_dir(out_dir)
        .extern_path(".event_store.client.Empty", "()")
        .extern_path(
            ".event_store.client.StreamIdentifier",
            "crate::event_store::generated::common::StreamIdentifier",
        )
        .extern_path(
            ".event_store.client.AllStreamPosition",
            "crate::event_store::generated::common::AllStreamPosition",
        )
        .extern_path(
            ".event_store.client.UUID",
            "crate::event_store::generated::common::Uuid",
        )
        .compile_protos(&files, &["protos"])?;

    for entry in std::fs::read_dir(out_dir)? {
        let file = entry.unwrap();
        let filename_string = file.file_name().into_string().expect("valid utf-8 string");
        if filename_string.starts_with("event_store.client.") {
            let remaining = filename_string.trim_start_matches("event_store.client.");
            let new_file_name = if remaining == "persistent_subscriptions.rs" {
                "persistent.rs"
            } else {
                if remaining == "rs" || remaining == "client.rs" {
                    fs::remove_file(file.path())?;
                    continue;
                }

                remaining
            };

            let new_file = file.path().parent().unwrap().join(new_file_name);

            fs::rename(file.path(), new_file)?;
        } else if filename_string.as_str() == "google.rpc.rs" {
            let new_file = file.path().parent().unwrap().join("google_rpc.rs");
            fs::rename(file.path(), new_file)?;
        }
    }

    Ok(())
}

fn main() {
    generate().unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
