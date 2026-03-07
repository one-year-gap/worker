-- Kafka 발송 성공 처리
UPDATE analysis_dispatch_outbox
SET
    status = 'SENT'::dispatch_outbox_status,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    last_error = NULL,
    updated_at = NOW()
WHERE request_id = :request_id;
