-- Kafka 발송 성공 처리
UPDATE analysis_dispatch_outbox
SET
    dispatch_status = 'SENT'::dispatch_status,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    last_error = NULL,
    claimed_done_at = NOW(),
    updated_at = NOW()
WHERE request_id = :request_id
  AND type = 'REQUEST'::dispatch_outbox_type;
