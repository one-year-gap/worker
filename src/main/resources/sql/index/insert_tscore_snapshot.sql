-- 목적:
-- 1) raw 스냅샷에서 지수별 평균/표준편차를 계산한다.
-- 2) T = 50 + 10 * Z 공식을 적용해 tscore를 적재한다.
-- 3) 표준편차가 0인 경우 분모 0 방지를 위해 50점으로 고정한다.
WITH raw_data AS (
    -- 특정 snapshot_date 데이터만 대상으로 한다.
    SELECT
        r.member_id,
        r.explore_raw,
        r.benefit_trend_raw,
        r.multi_device_raw,
        r.family_home_raw,
        r.internet_security_raw,
        r.stability_raw
    FROM index_raw_snapshot r
    WHERE r.snapshot_date = CAST(:snapshotDate AS date)
),
stats AS (
    -- 지수별 분포 통계량을 한 번에 계산한다.
    SELECT
        AVG(explore_raw) AS explore_mean,
        STDDEV_POP(explore_raw) AS explore_stddev,
        AVG(benefit_trend_raw) AS benefit_trend_mean,
        STDDEV_POP(benefit_trend_raw) AS benefit_trend_stddev,
        AVG(multi_device_raw) AS multi_device_mean,
        STDDEV_POP(multi_device_raw) AS multi_device_stddev,
        AVG(family_home_raw) AS family_home_mean,
        STDDEV_POP(family_home_raw) AS family_home_stddev,
        AVG(internet_security_raw) AS internet_security_mean,
        STDDEV_POP(internet_security_raw) AS internet_security_stddev,
        AVG(stability_raw) AS stability_mean,
        STDDEV_POP(stability_raw) AS stability_stddev
    FROM raw_data
)
INSERT INTO index_tscore_snapshot (
    snapshot_date,
    member_id,
    explore_tscore,
    benefit_trend_tscore,
    multi_device_tscore,
    family_home_tscore,
    internet_security_tscore,
    stability_tscore
)
SELECT
    CAST(:snapshotDate AS date) AS snapshot_date,
    r.member_id,
    -- stddev=0이면 모두 같은 값이므로 중립점수 50 부여
    CASE
        WHEN COALESCE(s.explore_stddev, 0) = 0 THEN 50
        ELSE 50 + 10 * ((r.explore_raw - s.explore_mean) / s.explore_stddev)
    END AS explore_tscore,
    CASE
        WHEN COALESCE(s.benefit_trend_stddev, 0) = 0 THEN 50
        ELSE 50 + 10 * ((r.benefit_trend_raw - s.benefit_trend_mean) / s.benefit_trend_stddev)
    END AS benefit_trend_tscore,
    CASE
        WHEN COALESCE(s.multi_device_stddev, 0) = 0 THEN 50
        ELSE 50 + 10 * ((r.multi_device_raw - s.multi_device_mean) / s.multi_device_stddev)
    END AS multi_device_tscore,
    CASE
        WHEN COALESCE(s.family_home_stddev, 0) = 0 THEN 50
        ELSE 50 + 10 * ((r.family_home_raw - s.family_home_mean) / s.family_home_stddev)
    END AS family_home_tscore,
    CASE
        WHEN COALESCE(s.internet_security_stddev, 0) = 0 THEN 50
        ELSE 50 + 10 * ((r.internet_security_raw - s.internet_security_mean) / s.internet_security_stddev)
    END AS internet_security_tscore,
    CASE
        WHEN COALESCE(s.stability_stddev, 0) = 0 THEN 50
        ELSE 50 + 10 * ((r.stability_raw - s.stability_mean) / s.stability_stddev)
    END AS stability_tscore
FROM raw_data r
CROSS JOIN stats s;
