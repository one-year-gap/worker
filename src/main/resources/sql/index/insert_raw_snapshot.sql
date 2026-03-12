-- 목적:
-- 1) 회원 전체를 모집단으로 6개 raw 지수를 계산한다.
-- 2) 로그/월 데이터 누락 회원도 포함하기 위해 LEFT JOIN + COALESCE 전략을 쓴다.
-- 3) 같은 snapshot_date 재실행 시 결과를 재현할 수 있도록 set-based 일괄 적재를 사용한다.
WITH params AS (
    -- 배치 기준 날짜와 월 키를 한 곳에서 고정한다.
    SELECT
        CAST(:snapshotDate AS date) AS snapshot_date,
        CAST(:yyyymm AS varchar(6)) AS yyyymm
),
base_members AS (
    -- 모집단은 member 전체다.
    SELECT
        m.member_id,
        m.membership,
        COALESCE(m.children_count, 0) AS children_count,
        m.family_group_id
    FROM member m
),
family_counts AS (
    -- 가족 결합도 계산용: family_group_id별 인원 수
    SELECT
        m.family_group_id,
        COUNT(*)::bigint AS family_member_cnt
    FROM member m
    WHERE m.family_group_id IS NOT NULL
    GROUP BY m.family_group_id
),
active_subscriptions AS (
    -- 스냅샷 시점에 유효한 활성 구독만 선택한다.
    SELECT
        s.member_id,
        s.subscription_id,
        s.product_id,
        p.product_type,
        p.name
    FROM subscription s
    JOIN product p ON p.product_id = s.product_id
    JOIN params pr ON TRUE
    WHERE s.status = TRUE
      AND s.start_date::date <= pr.snapshot_date
      AND (s.end_date IS NULL OR s.end_date::date >= pr.snapshot_date)
),
addon_features AS (
    -- 혜택/보안 지수 계산용 부가서비스 피처
    SELECT
        a.member_id,
        COUNT(*) FILTER (WHERE ad.addon_type::text = 'SECURITY')::bigint AS security_addon_cnt,
        COUNT(*)::bigint AS addon_subscribe_cnt
    FROM active_subscriptions a
    JOIN addon_service ad ON ad.product_id = a.product_id
    GROUP BY a.member_id
),
internet_iptv_features AS (
    -- 인터넷 가입 여부 + 인터넷/IPTV 동시 가입 여부
    SELECT
        a.member_id,
        CASE WHEN COUNT(*) FILTER (WHERE a.product_type::text = 'INTERNET') > 0 THEN 1 ELSE 0 END AS internet_flag,
        CASE
            WHEN COUNT(*) FILTER (WHERE a.product_type::text = 'INTERNET') > 0
             AND COUNT(*) FILTER (WHERE a.product_type::text = 'IPTV') > 0
            THEN 1 ELSE 0
        END AS has_internet_iptv_bundle
    FROM active_subscriptions a
    GROUP BY a.member_id
),
watch_tablet_features AS (
    -- TAB_WATCH_PLAN 중 이름 패턴으로 워치/태블릿을 구분한다.
    SELECT
        a.member_id,
        CASE
            WHEN COUNT(*) FILTER (
                WHERE a.product_type::text = 'TAB_WATCH_PLAN'
                  AND (
                      a.name ILIKE '%watch%'
                      OR a.name ILIKE '%wearable%'
                  )
            ) > 0 THEN 1 ELSE 0
        END AS watch_flag,
        CASE
            WHEN COUNT(*) FILTER (
                WHERE a.product_type::text = 'TAB_WATCH_PLAN'
                  AND NOT (
                      a.name ILIKE '%watch%'
                      OR a.name ILIKE '%wearable%'
                  )
            ) > 0 THEN 1 ELSE 0
        END AS tablet_flag
    FROM active_subscriptions a
    GROUP BY a.member_id
),
mobile_limit AS (
    -- 제공량 문자열에서 숫자만 추출해 GB 한도로 파싱한다.
    -- 이유: 실제 데이터가 "기본제공량 내 ... 55GB" 같은 문자열 포맷을 포함함.
    SELECT
        a.member_id,
        COALESCE(
            MAX(
                CASE
                    WHEN mp.tethering_sharing_data IS NULL THEN NULL
                    WHEN regexp_replace(mp.tethering_sharing_data::text, '[^0-9.]', '', 'g')
                         ~ '^[0-9]+(\\.[0-9]+)?$'
                    THEN regexp_replace(mp.tethering_sharing_data::text, '[^0-9.]', '', 'g')::numeric
                    ELSE NULL
                END
            ),
            0
        ) AS sharing_limit_gb
    FROM active_subscriptions a
    JOIN mobile_plan mp ON mp.product_id = a.product_id
    WHERE a.product_type::text = 'MOBILE_PLAN'
    GROUP BY a.member_id
),
sharing_used AS (
    -- usage JSON에서 tethering_sharing_data_gb를 숫자형으로 안전 추출한다.
    SELECT
        a.member_id,
        SUM(
            CASE
                WHEN jsonb_exists(um.usage_details, 'tethering_sharing_data_gb')
                     AND (um.usage_details ->> 'tethering_sharing_data_gb') ~ '^[0-9]+(\\.[0-9]+)?$'
                THEN (um.usage_details ->> 'tethering_sharing_data_gb')::numeric
                ELSE 0
            END
        ) AS sharing_used_gb
    FROM active_subscriptions a
    JOIN usage_monthly um ON um.subscription_id = a.subscription_id
    JOIN params pr ON TRUE
    WHERE a.product_type::text = 'MOBILE_PLAN'
      AND um.yyyymm = pr.yyyymm
    GROUP BY a.member_id
),
coupon_7d AS (
    -- 스냅샷 기준 최근 7일 쿠폰 사용 건수
    SELECT
        mc.member_id,
        COUNT(*)::bigint AS coupon_used_cnt
    FROM member_coupon mc
    JOIN params pr ON TRUE
    WHERE mc.is_used = TRUE
      AND mc.used_at IS NOT NULL
      AND mc.used_at::date BETWEEN (pr.snapshot_date - INTERVAL '6 day')::date AND pr.snapshot_date
    GROUP BY mc.member_id
),
support_monthly AS (
    -- 스냅샷 월(yyyymm) 상담 만족도 평균
    SELECT
        sc.member_id,
        AVG(sc.satisfaction_score::numeric) AS avg_monthly_satisfaction
    FROM support_case sc
    JOIN params pr ON TRUE
    WHERE sc.satisfaction_score IS NOT NULL
      AND to_char(COALESCE(sc.resolved_at, sc.updated_at), 'YYYYMM') = pr.yyyymm
    GROUP BY sc.member_id
),
billing_monthly AS (
    -- 스냅샷 월 납부 상태
    SELECT
        b.member_id,
        b.is_paid
    FROM billing b
    JOIN params pr ON TRUE
    WHERE b.yyyymm = pr.yyyymm
),
log_features AS (
    -- Athena 집계 적재본에서 탐색/액션 관련 로그 피처를 가져온다.
    SELECT
        l.member_id,
        l.click_product_detail_cnt,
        l.click_compare_cnt,
        l.click_change_success_cnt
    FROM user_event_features_7d l
    JOIN params pr ON l.snapshot_date = pr.snapshot_date
)
INSERT INTO index_raw_snapshot (
    snapshot_date,
    member_id,
    explore_raw,
    benefit_trend_raw,
    multi_device_raw,
    family_home_raw,
    internet_security_raw,
    stability_raw
)
SELECT
    pr.snapshot_date AS snapshot_date,
    bm.member_id,
    -- 탐색 지수: 상세조회 + 비교(가중치 3)
    (
        LN(1 + COALESCE(lf.click_product_detail_cnt, 0)::numeric)
        + 3 * LN(1 + COALESCE(lf.click_compare_cnt, 0)::numeric)
    ) AS explore_raw,
    -- 혜택/트렌드: 부가가입 + 쿠폰사용 + 성공액션
    (
        LN(1 + COALESCE(af.addon_subscribe_cnt, 0)::numeric)
        + LN(1 + COALESCE(c7.coupon_used_cnt, 0)::numeric)
        + LN(1 + COALESCE(lf.click_change_success_cnt, 0)::numeric)
    ) AS benefit_trend_raw,
    -- 멀티 디바이스: 워치/태블릿 플래그 + sharing_rate(0~1 clamp)
    (
        COALESCE(wtf.watch_flag, 0)
        + COALESCE(wtf.tablet_flag, 0)
        + CASE
            WHEN COALESCE(ml.sharing_limit_gb, 0) > 0
            THEN LEAST(
                1.0::numeric,
                GREATEST(0.0::numeric, COALESCE(su.sharing_used_gb, 0) / ml.sharing_limit_gb)
            )
            ELSE 0.0::numeric
        END
    ) AS multi_device_raw,
    -- 가족/홈: 가족인원 + 홈결합 + 자녀수
    (
        LN(1 + COALESCE(fc.family_member_cnt, 0)::numeric)
        + COALESCE(iif.has_internet_iptv_bundle, 0)
        + LN(1 + COALESCE(bm.children_count, 0)::numeric)
    ) AS family_home_raw,
    -- 인터넷/보안: 인터넷 가입 신호(0.7) + 보안 부가서비스 신호(0.3)
    (
        0.7 * COALESCE(iif.internet_flag, 0)::numeric
        + 0.3 * LN(1 + COALESCE(af.security_addon_cnt, 0)::numeric)
    ) AS internet_security_raw,
    -- 안정성: 멤버십/상담평점/납부상태 평균
    (
        (
            CASE bm.membership
                WHEN 'VVIP' THEN 100
                WHEN 'VIP' THEN 80
                WHEN 'GOLD' THEN 60
                WHEN 'BASIC' THEN 40
                ELSE 0
            END
            + (COALESCE(sm.avg_monthly_satisfaction, 0) / 5.0) * 100
            + CASE WHEN COALESCE(bl.is_paid, FALSE) THEN 100 ELSE 0 END
        ) / 3.0
    ) AS stability_raw
FROM base_members bm
JOIN params pr ON TRUE
LEFT JOIN family_counts fc ON fc.family_group_id = bm.family_group_id
LEFT JOIN addon_features af ON af.member_id = bm.member_id
LEFT JOIN internet_iptv_features iif ON iif.member_id = bm.member_id
LEFT JOIN watch_tablet_features wtf ON wtf.member_id = bm.member_id
LEFT JOIN mobile_limit ml ON ml.member_id = bm.member_id
LEFT JOIN sharing_used su ON su.member_id = bm.member_id
LEFT JOIN coupon_7d c7 ON c7.member_id = bm.member_id
LEFT JOIN support_monthly sm ON sm.member_id = bm.member_id
LEFT JOIN billing_monthly bl ON bl.member_id = bm.member_id
LEFT JOIN log_features lf ON lf.member_id = bm.member_id;
