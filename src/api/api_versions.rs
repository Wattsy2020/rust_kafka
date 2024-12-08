use crate::api::request::KafkaRequest;
use super::response::{BaseKafkaResponse, KafkaResponse};

#[derive(Debug)]
pub struct ApiVersionsResponse {
    kafka_response: BaseKafkaResponse,
    error_code: ApiVersionsErrorCode,
}

impl ApiVersionsResponse {
    // in future this should take a specific ApiVersionsRequest
    pub fn process_request(request: &KafkaRequest) -> Self {
        ApiVersionsResponse {
            kafka_response: BaseKafkaResponse::new(request),
            error_code: match request.api_version() {
                0..=4 => ApiVersionsErrorCode::NoError,
                _ => ApiVersionsErrorCode::UnsupportedVersion
            }
        }
    }
}

impl KafkaResponse for ApiVersionsResponse {
    fn to_bytes(&self) -> impl Iterator<Item=u8> {
        self.kafka_response.to_bytes()
            .chain(self.error_code.to_error_code_int().to_be_bytes())
    }
}

#[derive(Debug)]
enum ApiVersionsErrorCode {
    NoError,
    UnsupportedVersion,
}

impl ApiVersionsErrorCode {
    pub fn to_error_code_int(&self) -> i16 {
        match self {
            ApiVersionsErrorCode::NoError => 0,
            ApiVersionsErrorCode::UnsupportedVersion => 35
        }
    }
}