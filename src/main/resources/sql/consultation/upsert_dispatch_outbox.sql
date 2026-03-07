-- Dispatch 전 Outbox 레코드 생성/갱신
INSERT INTO analysis_dispatch_outbox (
    request_id,
    job_instance_id,
    chunk_id,
    analysis_version,
    status,
    attempt_count,
    next_retry_at,
    last_error,
    created_at,
    updated_at,
    type
)
VALUES (
    :request_id,
    :job_instance_id,
    :chunk_id,
    :analysis_version,
    'READY'::dispatch_outbox_status,
    0,
    NULL,
    NULL,
    NOW(),
    NOW(),
    'REQUEST'::dispatch_outbox_type
)
ON CONFLICT (request_id) DO UPDATE
SET
    job_instance_id = EXCLUDED.job_instance_id,
    chunk_id = EXCLUDED.chunk_id,
    analysis_version = EXCLUDED.analysis_version,
    updated_at = NOW()
RETURNING status::text;
