-- 목적:
-- 1) 회원별 6개 T-score를 세로로 펼친 뒤 최고 점수 지수 1개를 선택한다.
-- 2) 선택된 지수 코드를 persona character_name으로 변환한다.
-- 3) persona_type(활성 버전 우선)에서 persona_type_id를 찾아 FK로 저장한다.
--
-- 동점 처리:
-- - tscore DESC, index_code ASC
-- - 즉, 점수가 같으면 index_code 사전순이 우선된다.
WITH expanded AS (
    SELECT
        t.member_id,
        v.index_code,
        v.tscore
    FROM index_tscore_snapshot t
    CROSS JOIN LATERAL (
        VALUES
            ('explore', t.explore_tscore),
            ('benefit_trend', t.benefit_trend_tscore),
            ('multi_device', t.multi_device_tscore),
            ('family_home', t.family_home_tscore),
            ('internet_security', t.internet_security_tscore),
            ('stability', t.stability_tscore)
    ) v(index_code, tscore)
    WHERE t.snapshot_date = CAST(:snapshotDate AS date)
),
ranked AS (
    SELECT
        e.member_id,
        e.index_code,
        e.tscore,
        ROW_NUMBER() OVER (
            PARTITION BY e.member_id
            ORDER BY e.tscore DESC, e.index_code ASC
        ) AS rn
    FROM expanded e
),
winners AS (
    SELECT
        r.member_id,
        r.index_code AS source_index_code,
        r.tscore AS source_tscore,
        CASE r.index_code
            WHEN 'explore' THEN 'SPACE_SHERLOCK'
            WHEN 'benefit_trend' THEN 'SPACE_SURFER'
            WHEN 'multi_device' THEN 'SPACE_OCTOPUS'
            WHEN 'family_home' THEN 'SPACE_GRAVITY'
            WHEN 'internet_security' THEN 'SPACE_GUARDIAN'
            WHEN 'stability' THEN 'SPACE_EXPLORER'
            ELSE 'SPACE_SHERLOCK'
        END AS persona_code
    FROM ranked r
    WHERE r.rn = 1
),
latest_persona AS (
    SELECT DISTINCT ON (p.character_name)
        p.character_name,
        p.persona_type_id
    FROM persona_type p
    WHERE p.is_active = TRUE
    ORDER BY p.character_name, p.version DESC, p.persona_type_id DESC
)
INSERT INTO index_persona_snapshot (
    snapshot_date,
    member_id,
    persona_type_id,
    persona_code,
    source_index_code,
    source_tscore
)
SELECT
    CAST(:snapshotDate AS date) AS snapshot_date,
    w.member_id,
    lp.persona_type_id,
    w.persona_code,
    w.source_index_code,
    w.source_tscore
FROM winners w
JOIN latest_persona lp
  ON lp.character_name = w.persona_code;