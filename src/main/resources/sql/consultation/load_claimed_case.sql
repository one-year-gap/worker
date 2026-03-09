SELECT
    ca.analysis_id,
    ca.case_id,
    ca.analyzer_version
FROM
    analysis_dispatch_outbox ob
JOIN
    consultation_analysis ca
    ON ob.request_id = encode(sha256((ca.case_id::text || ':' || ca.analyzer_version::text)::bytea), 'hex')
WHERE
    ob.claim_token = :claim_token
    AND ob.type = 'REQUEST'::dispatch_outbox_type
    AND ob.dispatch_status IN ('READY'::dispatch_status, 'RETRY'::dispatch_status)
    AND ob.claimed_done_at IS NULL
ORDER BY ca.analysis_id;
