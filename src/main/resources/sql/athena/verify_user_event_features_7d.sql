SELECT COUNT(*)
FROM user_event_features_7d
WHERE snapshot_date = CAST(:snapshotDate AS date)
