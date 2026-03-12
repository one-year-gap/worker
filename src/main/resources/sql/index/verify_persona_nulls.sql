-- 목적: persona 스냅샷 필수 컬럼 null 여부 검증
SELECT COUNT(*) AS null_count
FROM index_persona_snapshot
WHERE snapshot_date = CAST(:snapshotDate AS date)
  AND (
      persona_type_id IS NULL
      OR source_index_code IS NULL
      OR source_tscore IS NULL
  );