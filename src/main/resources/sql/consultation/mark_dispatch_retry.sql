-- Kafka 전송 실패 처리
-- : 최대 도달 시도 시 DEAD 처리
-- : 최대 도달이 아니면 RETRY+지수 백오프 1m->2m->4m
UPDATE analysis_dispatch_outbox
SET
    attempt_count = attempt_count + 1,
    dispatch_status = CASE
                          WHEN attempt_count + 1 >= :max_attempts THEN 'DEAD'::dispatch_status
                          ELSE 'RETRY'::dispatch_status
                      END,
    next_retry_at = CASE
                        WHEN attempt_count + 1 >= :max_attempts THEN NULL
                        ELSE NOW() + make_interval(secs => CAST(60 * POWER(2, GREATEST(attempt_count, 0)) AS INT))
                    END,
    last_error = :last_error,
    claimed_done_at = NOW(),
    updated_at = NOW()
WHERE request_id = :request_id
  AND type = 'REQUEST'::dispatch_outbox_type
RETURNING dispatch_status::text;
