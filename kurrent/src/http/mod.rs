pub mod persistent_subscriptions;

pub fn http_configure_auth(
    builder: reqwest::RequestBuilder,
    creds_opt: Option<&crate::Credentials>,
) -> reqwest::RequestBuilder {
    if let Some(creds) = creds_opt {
        builder.basic_auth(
            unsafe { std::str::from_utf8_unchecked(creds.login.as_ref()) },
            unsafe { Some(std::str::from_utf8_unchecked(creds.password.as_ref())) },
        )
    } else {
        builder
    }
}

pub async fn http_execute_request(
    builder: reqwest::RequestBuilder,
) -> crate::Result<reqwest::Response> {
    let resp = builder.send().await.map_err(|e| {
        if let Some(status) = e.status() {
            match status {
                http::StatusCode::UNAUTHORIZED => crate::Error::AccessDenied,
                http::StatusCode::NOT_FOUND => crate::Error::ResourceNotFound,
                code if code.is_server_error() => crate::Error::ServerError(e.to_string()),
                code => {
                    error!(
                        "Unexpected error when dealing with HTTP request to the server: Code={:?}, {}",
                        code,
                        e
                    );
                    crate::Error::InternalClientError
                }
            }
        } else {
            error!(
                "Unexpected error when dealing with HTTP request to the server: {}",
                e,
            );

            crate::Error::InternalClientError
        }
    })?;

    if resp.status().is_success() {
        return Ok(resp);
    }

    let code = resp.status();
    let msg = resp.text().await.unwrap_or_else(|_| "".to_string());

    match code {
        http::StatusCode::UNAUTHORIZED => Err(crate::Error::AccessDenied),
        http::StatusCode::NOT_FOUND => Err(crate::Error::ResourceNotFound),
        code if code.is_server_error() => Err(crate::Error::ServerError(format!(
            "unexpected server error, reason: {:?}",
            code.canonical_reason()
        ))),
        code => {
            error!(
                "Unexpected error when dealing with HTTP request to the server: Code={:?}: {}",
                code, msg,
            );
            Err(crate::Error::InternalClientError)
        }
    }
}
