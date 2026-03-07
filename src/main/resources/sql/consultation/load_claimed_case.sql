SELECT
    ca.analysis_id,
    ca.case_id,
    ca.analyzer_version
FROM
    consultation_analysis as ca
WHERE
    ca.claim_token = :claim_token
    AND ca.analysis_status = 'IN_PROGRESS'
ORDER BY ca.analysis_id;
