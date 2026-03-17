-- member_llm_context를 한 번에 계산해서 upsert한다.
--
-- 핵심 원칙:
-- 1) 적재 대상은 현재 활성 MOBILE_PLAN을 가진 회원만 본다.
-- 2) persona / tscore / churn은 회원별 최신 스냅샷 1건을 붙인다.
-- 3) 로그는 snapshotDate 기준 최근 14일 내 최신 1건만 붙인다.
-- 4) usage_monthly는 snapshotDate의 직전 달 yyyymm을 사용한다.
WITH base_members AS (
    -- member 전체에서 기본 회원 속성을 가져온다.
    SELECT
        m.member_id,
        m.membership,
        m.birth_date,
        COALESCE(m.children_count, 0) AS children_count,
        m.family_group_id,
        m.family_role
    FROM member m
),
active_subscriptions AS (
    -- snapshotDate 시점에 활성 상태인 구독만 남긴다.
    -- current_subscriptions / current_product_types / 약정 계산의 공통 입력이다.
    SELECT
        s.subscription_id,
        s.member_id,
        s.product_id,
        s.start_date,
        s.end_date,
        s.contract_end_date,
        p.name AS product_name,
        p.product_type::text AS product_type
    FROM subscription s
    JOIN product p ON p.product_id = s.product_id
    WHERE s.status = TRUE
      AND s.start_date::date <= CAST(:snapshotDate AS date)
      AND (s.end_date IS NULL OR s.end_date::date >= CAST(:snapshotDate AS date))
),
mobile_plan_subscription AS (
    -- LLM 컨텍스트 적재 대상은 활성 MOBILE_PLAN 보유 회원이다.
    -- 회원당 모바일 플랜은 1개라고 가정하지만, 방어적으로 start_date ASC 1건을 고른다.
    SELECT DISTINCT ON (a.member_id)
        a.member_id,
        a.subscription_id,
        a.start_date,
        mp.data_amount
    FROM active_subscriptions a
    JOIN mobile_plan mp ON mp.product_id = a.product_id
    WHERE a.product_type = 'MOBILE_PLAN'
    ORDER BY a.member_id, a.start_date ASC
),
family_counts AS (
    -- 같은 family_group_id를 가진 회원 수를 계산한다.
    -- 자기 자신도 포함한다.
    SELECT
        m.family_group_id,
        COUNT(*)::int AS family_group_num
    FROM member m
    WHERE m.family_group_id IS NOT NULL
    GROUP BY m.family_group_id
),
latest_persona AS (
    -- 회원별 최신 persona_code 1건.
    SELECT DISTINCT ON (s.member_id)
        s.member_id,
        s.persona_code
    FROM index_persona_snapshot s
    ORDER BY s.member_id, s.snapshot_date DESC
),
latest_tscore AS (
    -- 회원별 최신 tscore 1건.
    -- segment의 upsell 판정에 사용한다.
    SELECT DISTINCT ON (t.member_id)
        t.member_id,
        t.explore_tscore,
        t.benefit_trend_tscore,
        t.multi_device_tscore,
        t.family_home_tscore
    FROM index_tscore_snapshot t
    ORDER BY t.member_id, t.snapshot_date DESC
),
latest_churn AS (
    -- 회원별 최신 churn 스냅샷 1건.
    -- 현재는 churn_score / churn_tier 적재와 CHURN_RISK 분기에 사용한다.
    SELECT DISTINCT ON (c.member_id)
        c.member_id,
        c.churn_score,
        c.risk_level AS churn_tier
    FROM churn_score_snapshot c
    ORDER BY c.member_id, c.base_date DESC
),
latest_logs_14d AS (
    -- 행동 로그는 최근 14일 이내 데이터만 허용하고 그중 최신 1건을 사용한다.
    SELECT DISTINCT ON (l.member_id)
        l.member_id,
        l.product_type_clicks,
        l.product_type_top_tags
    FROM user_event_features_7d l
    WHERE l.snapshot_date <= CAST(:snapshotDate AS date)
      AND l.snapshot_date >= CAST(:snapshotDate AS date) - INTERVAL '14 days'
    ORDER BY l.member_id, l.snapshot_date DESC
),
recent_counseling AS (
    -- 회원별 최신 상담 제목 최대 3개를 ' | '로 묶는다.
    SELECT
        x.member_id,
        string_agg(x.title, ' | ' ORDER BY x.created_at DESC) AS recent_counseling
    FROM (
        SELECT
            sc.member_id,
            sc.title,
            sc.created_at,
            row_number() OVER (PARTITION BY sc.member_id ORDER BY sc.created_at DESC) AS rn
        FROM support_case sc
        WHERE sc.title IS NOT NULL
          AND btrim(sc.title) <> ''
    ) x
    WHERE x.rn <= 3
    GROUP BY x.member_id
),
current_subscriptions_json AS (
    -- 현재 활성 구독 전체를 JSON 배열로 만든다.
    -- product type 5종을 모두 포함한다.
    SELECT
        a.member_id,
        jsonb_agg(
            jsonb_build_object(
                'product_id', a.product_id,
                'product_name', a.product_name,
                'product_type', a.product_type,
                'start_date', to_char(a.start_date::date, 'YYYY-MM-DD')
            )
            ORDER BY a.start_date ASC
        ) AS current_subscriptions
    FROM active_subscriptions a
    GROUP BY a.member_id
),
current_product_types_json AS (
    -- 현재 구독 중인 product_type 보유 여부를 고정 키 boolean map으로 만든다.
    SELECT
        a.member_id,
        jsonb_build_object(
            'MOBILE_PLAN', COALESCE(bool_or(a.product_type = 'MOBILE_PLAN'), FALSE),
            'INTERNET', COALESCE(bool_or(a.product_type = 'INTERNET'), FALSE),
            'IPTV', COALESCE(bool_or(a.product_type = 'IPTV'), FALSE),
            'TAB_WATCH_PLAN', COALESCE(bool_or(a.product_type = 'TAB_WATCH_PLAN'), FALSE),
            'ADDON', COALESCE(bool_or(a.product_type = 'ADDON'), FALSE)
        ) AS current_product_types
    FROM active_subscriptions a
    GROUP BY a.member_id
),
usage_base AS (
    -- 모바일 플랜의 데이터 제공량과 전월 사용량을 한곳에 모은다.
    -- data_amount가 순수 NGB 형식이 아니면 비교 불가로 본다.
    SELECT
        ms.member_id,
        CASE
            WHEN ms.data_amount ~ '^[0-9]+(\.[0-9]+)?GB$' THEN regexp_replace(ms.data_amount, 'GB$', '', 'g')::numeric
            ELSE NULL
        END AS allowance_gb,
        CASE
            WHEN jsonb_typeof(um.usage_details) = 'object'
                 AND (um.usage_details ->> 'data_gb') ~ '^[0-9]+(\.[0-9]+)?$'
            THEN (um.usage_details ->> 'data_gb')::numeric
            ELSE NULL
        END AS data_gb
    FROM mobile_plan_subscription ms
    LEFT JOIN usage_monthly um
      ON um.subscription_id = ms.subscription_id
     AND um.yyyymm = CAST(:yyyymm AS varchar(6))
),
usage_metrics AS (
    -- usage ratio는 내부 계산용 원본 비율이다.
    -- current_data_usage_ratio는 여기에서 *100 후 반올림해서 정수화한다.
    SELECT
        u.member_id,
        CASE
            WHEN u.allowance_gb IS NULL OR u.allowance_gb = 0 OR u.data_gb IS NULL THEN NULL
            ELSE u.data_gb / u.allowance_gb
        END AS usage_ratio
    FROM usage_base u
),
contract_flags AS (
    -- ADDON을 제외한 plan 계열 구독 중 3개월 이내 약정 만료 여부를 계산한다.
    SELECT
        a.member_id,
        bool_or(
            a.product_type <> 'ADDON'
            AND a.contract_end_date IS NOT NULL
            AND a.contract_end_date::date >= CAST(:snapshotDate AS date)
            AND a.contract_end_date::date < CAST(:snapshotDate AS date) + INTERVAL '3 months'
        ) AS contract_expiry_within_3m
    FROM active_subscriptions a
    GROUP BY a.member_id
),
final_rows AS (
    -- 최종 적재 대상 1행을 회원별로 만든다.
    -- 이 단계에서 컬럼별 최종 값과 segment를 모두 계산한다.
    SELECT
        bm.member_id,
        bm.membership,
        concat(
            ((extract(year FROM age(CAST(:snapshotDate AS date), bm.birth_date))::int / 10) * 10),
            chr(45824)
        ) AS age_group,
        (
            extract(year FROM age(CAST(:snapshotDate AS date), ms.start_date::date))::int * 12
            + extract(month FROM age(CAST(:snapshotDate AS date), ms.start_date::date))::int
        ) AS join_months,
        bm.children_count,
        COALESCE(fc.family_group_num, 0) AS family_group_num,
        bm.family_role,
        lp.persona_code,
        COALESCE(csj.current_subscriptions, '[]'::jsonb) AS current_subscriptions,
        COALESCE(
            cpt.current_product_types,
            jsonb_build_object(
                'MOBILE_PLAN', FALSE,
                'INTERNET', FALSE,
                'IPTV', FALSE,
                'TAB_WATCH_PLAN', FALSE,
                'ADDON', FALSE
            )
        ) AS current_product_types,
        CASE
            WHEN um.usage_ratio IS NULL THEN NULL
            ELSE round(um.usage_ratio * 100)::int
        END AS current_data_usage_ratio,
        CASE
            WHEN um.usage_ratio IS NULL THEN NULL
            WHEN um.usage_ratio > 1.0 THEN 'OVER'
            WHEN um.usage_ratio > 0.6 THEN 'FIT'
            ELSE 'UNDER'
        END AS data_usage_pattern,
        lc.churn_score,
        lc.churn_tier,
        rc.recent_counseling,
        ll.product_type_clicks,
        COALESCE(ll.product_type_top_tags, '[]'::jsonb) AS recent_viewed_tags_top_3,
        COALESCE(cf.contract_expiry_within_3m, FALSE) AS contract_expiry_within_3m,
        CASE
            WHEN lc.churn_tier = 'HIGH' THEN 'CHURN_RISK'
            WHEN (
                (CASE
                    WHEN um.usage_ratio IS NULL THEN NULL
                    WHEN um.usage_ratio > 1.0 THEN 'OVER'
                    WHEN um.usage_ratio > 0.6 THEN 'FIT'
                    ELSE 'UNDER'
                END) = 'OVER'
                OR (
                    SELECT COUNT(*)
                    FROM jsonb_array_elements_text(COALESCE(ll.product_type_top_tags, '[]'::jsonb)) AS t(tag)
                    WHERE t.tag IN (
                        U&'OTT\D504\B9AC\BBF8\C5C4',
                        U&'\AC00\C871\ACB0\D569\BA54\C778',
                        U&'\AC00\C871\ACF5\C720',
                        U&'\D14C\B354\B9C1\C250\C5B4\B9C1'
                    )
                ) >= 2
                OR COALESCE(lt.benefit_trend_tscore, 0) >= 55
                OR COALESCE(lt.explore_tscore, 0) >= 55
                OR COALESCE(lt.multi_device_tscore, 0) >= 60
                OR COALESCE(lt.family_home_tscore, 0) >= 60
            ) THEN 'UPSELL'
            ELSE 'NORMAL'
        END AS segment
    FROM base_members bm
    JOIN mobile_plan_subscription ms ON ms.member_id = bm.member_id
    LEFT JOIN family_counts fc ON fc.family_group_id = bm.family_group_id
    LEFT JOIN latest_persona lp ON lp.member_id = bm.member_id
    LEFT JOIN latest_tscore lt ON lt.member_id = bm.member_id
    LEFT JOIN latest_churn lc ON lc.member_id = bm.member_id
    LEFT JOIN latest_logs_14d ll ON ll.member_id = bm.member_id
    LEFT JOIN recent_counseling rc ON rc.member_id = bm.member_id
    LEFT JOIN current_subscriptions_json csj ON csj.member_id = bm.member_id
    LEFT JOIN current_product_types_json cpt ON cpt.member_id = bm.member_id
    LEFT JOIN usage_metrics um ON um.member_id = bm.member_id
    LEFT JOIN contract_flags cf ON cf.member_id = bm.member_id
)
INSERT INTO member_llm_context (
    member_id,
    membership,
    age_group,
    join_months,
    children_count,
    family_group_num,
    family_role,
    persona_code,
    segment,
    current_subscriptions,
    current_product_types,
    current_data_usage_ratio,
    data_usage_pattern,
    churn_score,
    churn_tier,
    recent_counseling,
    product_type_clicks,
    recent_viewed_tags_top_3,
    contract_expiry_within_3m,
    updated_at
)
SELECT
    member_id,
    membership,
    age_group,
    join_months,
    children_count,
    family_group_num,
    family_role,
    persona_code,
    segment,
    current_subscriptions,
    current_product_types,
    current_data_usage_ratio,
    data_usage_pattern,
    churn_score,
    churn_tier,
    recent_counseling,
    product_type_clicks,
    recent_viewed_tags_top_3,
    contract_expiry_within_3m,
    now()
FROM final_rows
ON CONFLICT (member_id) DO UPDATE SET
    membership = EXCLUDED.membership,
    age_group = EXCLUDED.age_group,
    join_months = EXCLUDED.join_months,
    children_count = EXCLUDED.children_count,
    family_group_num = EXCLUDED.family_group_num,
    family_role = EXCLUDED.family_role,
    persona_code = EXCLUDED.persona_code,
    segment = EXCLUDED.segment,
    current_subscriptions = EXCLUDED.current_subscriptions,
    current_product_types = EXCLUDED.current_product_types,
    current_data_usage_ratio = EXCLUDED.current_data_usage_ratio,
    data_usage_pattern = EXCLUDED.data_usage_pattern,
    churn_score = EXCLUDED.churn_score,
    churn_tier = EXCLUDED.churn_tier,
    recent_counseling = EXCLUDED.recent_counseling,
    product_type_clicks = EXCLUDED.product_type_clicks,
    recent_viewed_tags_top_3 = EXCLUDED.recent_viewed_tags_top_3,
    contract_expiry_within_3m = EXCLUDED.contract_expiry_within_3m,
    updated_at = EXCLUDED.updated_at;
