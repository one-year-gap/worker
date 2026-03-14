-- member_llm_context 최소 검증 SQL.
--
-- 현재 잡의 적재 대상은 "활성 MOBILE_PLAN 보유 회원"이므로
-- member 전체가 아니라 eligible_count와 비교해야 한다.
SELECT
    (
        SELECT COUNT(DISTINCT s.member_id)
        FROM subscription s
        JOIN product p ON p.product_id = s.product_id
        WHERE s.status = TRUE
          AND p.product_type::text = 'MOBILE_PLAN'
    ) AS eligible_count,
    (SELECT COUNT(*) FROM member_llm_context) AS context_count,
    (SELECT COUNT(*) FROM member_llm_context WHERE member_id IS NULL) AS null_pk_count,
    (
        SELECT COUNT(*)
        FROM member_llm_context
        WHERE segment NOT IN ('CHURN_RISK', 'UPSELL', 'NORMAL')
           OR segment IS NULL
    ) AS invalid_segment_count,
    (
        SELECT COUNT(*)
        FROM member_llm_context
        WHERE current_product_types IS NULL
    ) AS null_product_types_count;