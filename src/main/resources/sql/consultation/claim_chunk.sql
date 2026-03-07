-- params:
-- :ver        BIGINT  -- analyzer_version (현재 분석기 버전)
-- :job        BIGINT  -- job_instance_id
-- :chunk      INT     -- chunk 개수
-- :lease_sec  INT     -- IN_PROGRESS 회수 TTL(초)

WITH targets AS (
  -- 1) 현재 버전 row가 아예 없는 케이스
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
  -- 2) 신규 대상은 곧바로 선점 상태(IN_PROGRESS)로 삽입
  INSERT INTO consultation_analysis (
    job_instance_id,
    case_id,
    analyzer_version,
    analysis_status,
    claimed_started_at,
    claimed_done_at,
    error_message,
    claim_token
  )
  SELECT
    :job,
    t.case_id,
    :ver,
    'IN_PROGRESS'::analysis_status,
    now(),
    NULL,
    NULL,
    :claim_token
  FROM targets t
  ON CONFLICT (case_id, analyzer_version) DO NOTHING
  RETURNING analysis_id
),
remaining AS (
  -- 3) 신규 선점 개수를 제외한 나머지 슬롯 계산
  SELECT GREATEST(:chunk - (SELECT COUNT(*) FROM seed), 0) AS n
),
picked AS (
  -- 4) 기존 row 중에서만 추가 선점 (최대 remaining.n 개)
  SELECT ca.analysis_id
  FROM consultation_analysis ca
  CROSS JOIN remaining r
  WHERE ca.analyzer_version = :ver
    AND r.n > 0
    AND (
      ca.analysis_status IN ('READY', 'FAILED')
      OR (
        ca.analysis_status = 'IN_PROGRESS'
        AND ca.claimed_started_at < now() - make_interval(secs => :lease_sec)
      )
    )
  ORDER BY ca.analysis_id
  FOR UPDATE SKIP LOCKED
  LIMIT (SELECT n FROM remaining)
),
claimed_existing AS (
  -- 5) 기존 row 선점 확정
  UPDATE consultation_analysis ca
  SET
    job_instance_id     = :job,
    analysis_status     = 'IN_PROGRESS'::analysis_status,
    claimed_started_at  = now(),
    claimed_done_at     = NULL,
    error_message       = NULL,
    claim_token         = :claim_token
  FROM picked p
  WHERE ca.analysis_id = p.analysis_id
  RETURNING ca.analysis_id
)
-- 6) 신규 선점 + 기존 선점 결과 반환
SELECT analysis_id FROM seed
UNION ALL
SELECT analysis_id FROM claimed_existing
ORDER BY analysis_id;
