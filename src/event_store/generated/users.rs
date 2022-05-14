#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateReq {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<create_req::Options>,
}
/// Nested message and enum types in `CreateReq`.
pub mod create_req {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Options {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub password: ::prost::alloc::string::String,
        #[prost(string, tag = "3")]
        pub full_name: ::prost::alloc::string::String,
        #[prost(string, repeated, tag = "4")]
        pub groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateResp {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateReq {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<update_req::Options>,
}
/// Nested message and enum types in `UpdateReq`.
pub mod update_req {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Options {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub password: ::prost::alloc::string::String,
        #[prost(string, tag = "3")]
        pub full_name: ::prost::alloc::string::String,
        #[prost(string, repeated, tag = "4")]
        pub groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateResp {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteReq {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<delete_req::Options>,
}
/// Nested message and enum types in `DeleteReq`.
pub mod delete_req {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Options {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteResp {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnableReq {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<enable_req::Options>,
}
/// Nested message and enum types in `EnableReq`.
pub mod enable_req {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Options {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnableResp {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DisableReq {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<disable_req::Options>,
}
/// Nested message and enum types in `DisableReq`.
pub mod disable_req {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Options {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DisableResp {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DetailsReq {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<details_req::Options>,
}
/// Nested message and enum types in `DetailsReq`.
pub mod details_req {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Options {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DetailsResp {
    #[prost(message, optional, tag = "1")]
    pub user_details: ::core::option::Option<details_resp::UserDetails>,
}
/// Nested message and enum types in `DetailsResp`.
pub mod details_resp {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct UserDetails {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub full_name: ::prost::alloc::string::String,
        #[prost(string, repeated, tag = "3")]
        pub groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        #[prost(message, optional, tag = "4")]
        pub last_updated: ::core::option::Option<user_details::DateTime>,
        #[prost(bool, tag = "5")]
        pub disabled: bool,
    }
    /// Nested message and enum types in `UserDetails`.
    pub mod user_details {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct DateTime {
            #[prost(int64, tag = "1")]
            pub ticks_since_epoch: i64,
        }
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangePasswordReq {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<change_password_req::Options>,
}
/// Nested message and enum types in `ChangePasswordReq`.
pub mod change_password_req {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Options {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub current_password: ::prost::alloc::string::String,
        #[prost(string, tag = "3")]
        pub new_password: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangePasswordResp {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetPasswordReq {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<reset_password_req::Options>,
}
/// Nested message and enum types in `ResetPasswordReq`.
pub mod reset_password_req {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Options {
        #[prost(string, tag = "1")]
        pub login_name: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub new_password: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetPasswordResp {}
#[doc = r" Generated client implementations."]
pub mod users_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct UsersClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl UsersClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> UsersClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> UsersClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            UsersClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn create(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateReq>,
        ) -> Result<tonic::Response<super::CreateResp>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/event_store.client.users.Users/Create");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn update(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateReq>,
        ) -> Result<tonic::Response<super::UpdateResp>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/event_store.client.users.Users/Update");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteReq>,
        ) -> Result<tonic::Response<super::DeleteResp>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/event_store.client.users.Users/Delete");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn disable(
            &mut self,
            request: impl tonic::IntoRequest<super::DisableReq>,
        ) -> Result<tonic::Response<super::DisableResp>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/event_store.client.users.Users/Disable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn enable(
            &mut self,
            request: impl tonic::IntoRequest<super::EnableReq>,
        ) -> Result<tonic::Response<super::EnableResp>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/event_store.client.users.Users/Enable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn details(
            &mut self,
            request: impl tonic::IntoRequest<super::DetailsReq>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::DetailsResp>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/event_store.client.users.Users/Details");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        pub async fn change_password(
            &mut self,
            request: impl tonic::IntoRequest<super::ChangePasswordReq>,
        ) -> Result<tonic::Response<super::ChangePasswordResp>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/event_store.client.users.Users/ChangePassword",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn reset_password(
            &mut self,
            request: impl tonic::IntoRequest<super::ResetPasswordReq>,
        ) -> Result<tonic::Response<super::ResetPasswordResp>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/event_store.client.users.Users/ResetPassword",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}