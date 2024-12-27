use crate::options::CommonOperationOptions;
use crate::{ClientSettings, NodePreference};
use base64::Engine;

pub(crate) fn build_request_metadata(
    settings: &ClientSettings,
    options: &CommonOperationOptions,
) -> tonic::metadata::MetadataMap
where
{
    use tonic::metadata::MetadataValue;

    let mut metadata = tonic::metadata::MetadataMap::new();
    let credentials = options
        .credentials
        .as_ref()
        .or_else(|| settings.default_authenticated_user().as_ref());

    if let Some(creds) = credentials {
        let login = String::from_utf8_lossy(&creds.login).into_owned();
        let password = String::from_utf8_lossy(&creds.password).into_owned();

        let basic_auth_string =
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", login, password));
        let basic_auth = format!("Basic {}", basic_auth_string);
        let header_value = MetadataValue::try_from(basic_auth.as_str())
            .expect("Auth header value should be valid metadata header value");

        metadata.insert("authorization", header_value);
    }

    if options.requires_leader || settings.node_preference() == NodePreference::Leader {
        let header_value = MetadataValue::try_from("true").expect("valid metadata header value");
        metadata.insert("requires-leader", header_value);
    }

    if let Some(conn_name) = settings.connection_name.as_ref() {
        let header_value =
            MetadataValue::try_from(conn_name.as_str()).expect("valid metadata header value");
        metadata.insert("connection-name", header_value);
    }

    metadata
}
