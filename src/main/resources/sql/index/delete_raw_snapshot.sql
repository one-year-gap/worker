-- 목적: 동일 snapshot_date 재실행 시 raw 결과를 덮어쓰기 위해 기존 데이터를 삭제한다.
DELETE FROM index_raw_snapshot
WHERE snapshot_date = CAST(:snapshotDate AS date);
