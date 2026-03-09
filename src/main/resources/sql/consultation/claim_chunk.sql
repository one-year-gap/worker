-- params:
-- :ver         BIGINT  -- analyzer_version
-- :job         BIGINT  -- job_instance_id
-- :chunk       INT     -- claim chunk size
-- :lease_sec   INT     -- IN_PROGRESS lease ttl (sec)
-- :claim_token BIGINT  -- TSID

WITH targets AS (
  -- 1) 현재 버전 row가 없는 케이스를 먼저 seed
  SELECT sc.case_id
  FROM support_case sc
  WHERE NOT EXISTS (
    SELECT 1
    FROM consultation_analysis ca
    WHERE ca.case_id = sc.case_id
      AND ca.analyzer_version = :ver
  )
  ORDER BY sc.case_id
  LIMIT :chunk
),
seed AS (
  -- 2) consultation_analysis seed
  INSERT INTO consultation_analysis (
    job_instance_id,
    case_id,
    analyzer_version
  )
  SELECT
    :job,
    t.case_id,
    :ver
  FROM targets t
  ON CONFLICT (case_id, analyzer_version) DO NOTHING
  RETURNING analysis_id
),
pickable AS (
  -- 3) outbox 상태 기준으로 claim 가능한 대상을 잠금 기반으로 선택
  SELECT
    ca.analysis_id,
    ca.case_id,
    ca.analyzer_version,
    encode(sha256((ca.case_id::text || ':' || ca.analyzer_version::text)::bytea), 'hex') AS request_id
  FROM consultation_analysis ca
  LEFT JOIN analysis_dispatch_outbox ob
    ON ob.request_id = encode(sha256((ca.case_id::text || ':' || ca.analyzer_version::text)::bytea), 'hex')
   AND ob.type = 'REQUEST'::dispatch_outbox_type
  WHERE ca.analyzer_version = :ver
    AND (
      ob.request_id IS NULL
      OR (
        ob.dispatch_status IN ('READY'::dispatch_status, 'RETRY'::dispatch_status)
        AND (ob.next_retry_at IS NULL OR ob.next_retry_at <= NOW())
        AND (
          ob.claimed_started_at IS NULL
          OR ob.claimed_done_at IS NOT NULL
          OR ob.claimed_started_at < now() - make_interval(secs => :lease_sec)
        )
      )
    )
  ORDER BY ca.analysis_id
  FOR UPDATE OF ca SKIP LOCKED
  LIMIT :chunk
),
claimed_case AS (
  -- 4) consultation_analysis에 잡 인스턴스 갱신
  UPDATE consultation_analysis ca
  SET
    job_instance_id = :job,
    updated_at = now()
  FROM pickable p
  WHERE ca.analysis_id = p.analysis_id
  RETURNING
    p.analysis_id,
    p.case_id,
    p.analyzer_version,
    p.request_id
),
claimed_outbox AS (
  -- 5) outbox에 claim 상태를 기록 (source of truth)
  INSERT INTO analysis_dispatch_outbox (
    request_id,
    job_instance_id,
    chunk_id,
    analysis_version,
    dispatch_status,
    attempt_count,
    next_retry_at,
    last_error,
    created_at,
    updated_at,
    type,
    claim_token,
    claimed_started_at,
    claimed_done_at
  )
  SELECT
    cc.request_id,
    :job,
    :claim_token,
    cc.analyzer_version::text,
    'READY'::dispatch_status,
    0,
    NULL,
    NULL,
    NOW(),
    NOW(),
    'REQUEST'::dispatch_outbox_type,
    :claim_token,
    NOW(),
    NULL
  FROM claimed_case cc
  ON CONFLICT (request_id) DO UPDATE
  SET
    job_instance_id    = EXCLUDED.job_instance_id,
    chunk_id           = EXCLUDED.chunk_id,
    analysis_version   = EXCLUDED.analysis_version,
    dispatch_status    = 'READY'::dispatch_status,
    next_retry_at      = NULL,
    last_error         = NULL,
    updated_at         = NOW(),
    type               = 'REQUEST'::dispatch_outbox_type,
    claim_token        = EXCLUDED.claim_token,
    claimed_started_at = EXCLUDED.claimed_started_at,
    claimed_done_at    = NULL
  RETURNING request_id
)
SELECT analysis_id
FROM claimed_case
ORDER BY analysis_id;
