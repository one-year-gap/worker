-- 목적: tscore 필수 컬럼에 null 값이 있는지 확인한다.
SELECT COUNT(*) AS null_count
FROM index_tscore_snapshot
WHERE snapshot_date = CAST(:snapshotDate AS date)
  AND (
      explore_tscore IS NULL
      OR benefit_trend_tscore IS NULL
      OR multi_device_tscore IS NULL
      OR family_home_tscore IS NULL
      OR internet_security_tscore IS NULL
      OR stability_tscore IS NULL
  );
