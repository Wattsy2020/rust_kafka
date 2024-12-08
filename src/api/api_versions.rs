use crate::api::request::{ApiKey, KafkaRequest};
use crate::serialisation::ToKafkaBytes;
use super::response::BaseKafkaResponse;

#[derive(Debug)]
pub struct ApiVersionsResponse {
    base_response: BaseKafkaResponse,
    error_code: ApiVersionsErrorCode,
    api_keys: Vec<ApiVersionInfo>,
}

impl ApiVersionsResponse {
    // in future this should take a specific ApiVersionsRequest
    pub fn process_request(request: &KafkaRequest) -> Self {
        let base_response = BaseKafkaResponse::new(request);
        let error_code = match request.api_version() {
            0..=4 => ApiVersionsErrorCode::NoError,
            _ => ApiVersionsErrorCode::UnsupportedVersion
        };
        let api_keys = match error_code {
            ApiVersionsErrorCode::UnsupportedVersion => Vec::new(),
            // in future this shouldn't be hardcoded
            ApiVersionsErrorCode::NoError => vec![ApiVersionInfo {
                api_key: ApiKey::ApiVersions,
                min_version: 0,
                max_version: 4
            }]
        };
        ApiVersionsResponse {
            base_response,
            error_code,
            api_keys
        }
    }
}

impl ToKafkaBytes for ApiVersionsResponse {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item=u8> {
        self.base_response.to_kafka_bytes().into_iter()
            .chain(self.error_code.to_kafka_bytes())
            .chain(self.api_keys.to_kafka_bytes())
    }
}

#[derive(Debug)]
pub struct ApiVersionInfo {
    api_key: ApiKey,
    min_version: i16,
    max_version: i16,
}

impl ToKafkaBytes for ApiVersionInfo {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item=u8> {
        self.api_key.to_kafka_bytes().into_iter()
            .chain(self.min_version.to_kafka_bytes())
            .chain(self.max_version.to_kafka_bytes())
    }
}

#[derive(Debug)]
enum ApiVersionsErrorCode {
    NoError,
    UnsupportedVersion,
}

impl ToKafkaBytes for ApiVersionsErrorCode {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item=u8> {
        let error_code: i16 = match self {
            ApiVersionsErrorCode::NoError => 0,
            ApiVersionsErrorCode::UnsupportedVersion => 35
        };
        error_code.to_kafka_bytes()
    }
}
