-- 목적: 모집단(member) 대비 raw/tscore/persona 누락 여부를 건수로 검증한다.
SELECT
    (SELECT COUNT(*) FROM member) AS member_count,
    (SELECT COUNT(*) FROM index_raw_snapshot WHERE snapshot_date = CAST(:snapshotDate AS date)) AS raw_count,
    (SELECT COUNT(*) FROM index_tscore_snapshot WHERE snapshot_date = CAST(:snapshotDate AS date)) AS tscore_count,
    (SELECT COUNT(*) FROM index_persona_snapshot WHERE snapshot_date = CAST(:snapshotDate AS date)) AS persona_count;
