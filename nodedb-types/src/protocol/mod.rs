pub mod auth;
pub mod batch;
pub mod frames;
pub mod handshake;
pub mod opcodes;
pub mod request_fields;
pub mod text_fields;

pub use auth::{AuthMethod, AuthResponse};
pub use batch::{BatchDocument, BatchVector};
pub use frames::{ErrorPayload, NativeRequest, NativeResponse};
pub use handshake::{
    CAP_COLUMNAR, CAP_CRDT, CAP_FTS, CAP_GRAPHRAG, CAP_SPATIAL, CAP_STREAMING, CAP_TIMESERIES,
    DEFAULT_NATIVE_PORT, FRAME_HEADER_LEN, HELLO_ACK_MAGIC, HELLO_MAGIC, HelloAckFrame, HelloFrame,
    Limits, MAX_FRAME_SIZE, PROTO_VERSION,
};
pub use opcodes::{OpCode, ResponseStatus, UnknownOpCode};
pub use request_fields::RequestFields;
pub use text_fields::TextFields;

// ─── Tests (protocol-level integration) ────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn opcode_repr() {
        assert_eq!(OpCode::Auth as u8, 0x01);
        assert_eq!(OpCode::Sql as u8, 0x20);
        assert_eq!(OpCode::Begin as u8, 0x40);
        assert_eq!(OpCode::GraphHop as u8, 0x50);
        assert_eq!(OpCode::TextSearch as u8, 0x60);
        assert_eq!(OpCode::VectorBatchInsert as u8, 0x70);
    }

    #[test]
    fn opcode_is_write() {
        assert!(OpCode::PointPut.is_write());
        assert!(OpCode::PointDelete.is_write());
        assert!(OpCode::CrdtApply.is_write());
        assert!(OpCode::EdgePut.is_write());
        assert!(!OpCode::PointGet.is_write());
        assert!(!OpCode::Sql.is_write());
        assert!(!OpCode::VectorSearch.is_write());
        assert!(!OpCode::Ping.is_write());
    }

    #[test]
    fn response_status_repr() {
        assert_eq!(ResponseStatus::Ok as u8, 0);
        assert_eq!(ResponseStatus::Partial as u8, 1);
        assert_eq!(ResponseStatus::Error as u8, 2);
    }

    #[test]
    fn native_response_ok() {
        let r = NativeResponse::ok(42);
        assert_eq!(r.seq, 42);
        assert_eq!(r.status, ResponseStatus::Ok);
        assert!(r.error.is_none());
    }

    #[test]
    fn native_response_error() {
        let r = NativeResponse::error(1, "42P01", "collection not found");
        assert_eq!(r.status, ResponseStatus::Error);
        let e = r.error.unwrap();
        assert_eq!(e.code, "42P01");
        assert_eq!(e.message, "collection not found");
    }

    #[test]
    fn native_response_from_query_result() {
        let qr = crate::result::QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![vec![
                Value::String("u1".into()),
                Value::String("Alice".into()),
            ]],
            rows_affected: 0,
        };
        let r = NativeResponse::from_query_result(5, qr, 100);
        assert_eq!(r.seq, 5);
        assert_eq!(r.watermark_lsn, 100);
        assert_eq!(r.columns.as_ref().unwrap().len(), 2);
        assert_eq!(r.rows.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn native_response_status_row() {
        let r = NativeResponse::status_row(3, "OK");
        assert_eq!(r.columns.as_ref().unwrap(), &["status"]);
        assert_eq!(r.rows.as_ref().unwrap()[0][0].as_str(), Some("OK"));
    }

    #[test]
    fn msgpack_roundtrip_request() {
        let req = NativeRequest {
            op: OpCode::Sql,
            seq: 1,
            fields: RequestFields::Text(TextFields {
                sql: Some("SELECT 1".into()),
                ..Default::default()
            }),
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        let decoded: NativeRequest = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.op, OpCode::Sql);
        assert_eq!(decoded.seq, 1);
    }

    #[test]
    fn msgpack_roundtrip_response() {
        let resp = NativeResponse::from_query_result(
            7,
            crate::result::QueryResult {
                columns: vec!["x".into()],
                rows: vec![vec![Value::Integer(42)]],
                rows_affected: 0,
            },
            99,
        );
        let bytes = zerompk::to_msgpack_vec(&resp).unwrap();
        let decoded: NativeResponse = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.seq, 7);
        assert_eq!(decoded.watermark_lsn, 99);
        assert_eq!(decoded.rows.unwrap()[0][0].as_i64(), Some(42));
    }

    #[test]
    fn auth_method_variants() {
        let trust = AuthMethod::Trust {
            username: "admin".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&trust).unwrap();
        let decoded: AuthMethod = zerompk::from_msgpack(&bytes).unwrap();
        match decoded {
            AuthMethod::Trust { username } => assert_eq!(username, "admin"),
            _ => panic!("expected Trust variant"),
        }

        let pw = AuthMethod::Password {
            username: "user".into(),
            password: "secret".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&pw).unwrap();
        let decoded: AuthMethod = zerompk::from_msgpack(&bytes).unwrap();
        match decoded {
            AuthMethod::Password { username, password } => {
                assert_eq!(username, "user");
                assert_eq!(password, "secret");
            }
            _ => panic!("expected Password variant"),
        }
    }

    #[test]
    fn hello_frame_roundtrip() {
        let frame = HelloFrame {
            proto_min: 1,
            proto_max: 3,
            capabilities: CAP_STREAMING | CAP_GRAPHRAG | CAP_FTS,
        };
        let buf = frame.encode();
        let decoded = HelloFrame::decode(&buf).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_frame_bad_magic() {
        let mut buf = HelloFrame {
            proto_min: 1,
            proto_max: 1,
            capabilities: 0,
        }
        .encode();
        buf[0] = 0xFF;
        assert!(HelloFrame::decode(&buf).is_none());
    }

    #[test]
    fn hello_ack_frame_roundtrip_all_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: CAP_STREAMING | CAP_CRDT,
            server_version: "0.1.0-dev".into(),
            limits: Limits {
                max_vector_dim: Some(1536),
                max_top_k: Some(1000),
                max_scan_limit: Some(10_000),
                max_batch_size: Some(512),
                max_crdt_delta_bytes: Some(1 << 20),
                max_query_text_bytes: Some(4096),
                max_graph_depth: Some(16),
            },
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_frame_roundtrip_some_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: 0,
            server_version: "1.0.0".into(),
            limits: Limits {
                max_vector_dim: Some(768),
                max_top_k: None,
                max_scan_limit: None,
                max_batch_size: None,
                max_crdt_delta_bytes: None,
                max_query_text_bytes: None,
                max_graph_depth: None,
            },
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_frame_roundtrip_no_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: CAP_STREAMING,
            server_version: "0.2.0".into(),
            limits: Limits::default(),
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_bad_magic() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: 0,
            server_version: "x".into(),
            limits: Limits::default(),
        };
        let mut enc = frame.encode();
        enc[0] = 0xFF;
        assert!(HelloAckFrame::decode(&enc).is_none());
    }

    #[test]
    fn cap_bits_non_overlapping() {
        let all = CAP_STREAMING
            | CAP_GRAPHRAG
            | CAP_FTS
            | CAP_CRDT
            | CAP_SPATIAL
            | CAP_TIMESERIES
            | CAP_COLUMNAR;
        assert_eq!(all.count_ones(), 7);
    }

    /// vector_id overflow rejected in plan_builder (unit-test the narrowing logic).
    #[test]
    fn vector_id_overflow_rejected_in_plan_builder() {
        // Simulate the narrowing: u64 > u32::MAX must fail
        let wire_id: u64 = u32::MAX as u64 + 1;
        let result: Result<u32, _> = wire_id.try_into();
        assert!(result.is_err(), "u64 > u32::MAX must not fit in u32");

        // u32::MAX itself must succeed
        let wire_ok: u64 = u32::MAX as u64;
        let result2: Result<u32, _> = wire_ok.try_into();
        assert_eq!(result2.unwrap(), u32::MAX);
    }
}
