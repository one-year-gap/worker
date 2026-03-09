package site.holliverse.worker.batch.consultation.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ConsultationDispatchOutboxRepository {
    private final JdbcClient jdbcClient;
    private final ConsultationAnalysisSql sql;

    /**
     * Dispatch Outbox 초기 생성
     * @param requestId: 발송 Id
     * @param jobInstanceId: Batch Job Instance Id
     * @param chunkId: 청크 단위 고유 Id
     * @param analysisVersion: 분석기 version
     */
    public String upsertReadyAndGetStatus(
            String requestId,
            long jobInstanceId,
            String chunkId,
            String analysisVersion
    ) {
        return jdbcClient.sql(sql.upsertDispatchOutbox())
                .param("request_id", requestId)
                .param("job_instance_id", jobInstanceId)
                .param("chunk_id", chunkId)
                .param("analysis_version", analysisVersion)
                .query(String.class)
                .single();
    }

    /**
     * Dispatch 전송 처리
     * @param requestId: 발송 Id
     */
    public void markSent(String requestId) {
        jdbcClient.sql(sql.markDispatchSent())
                .param("request_id", requestId)
                .update();
    }

    /**
     * Dispatch 실패 시 재시도 요청 기록
     * @param requestId: 발송 Id
     * @param lastError: error message
     * @param maxAttempts: 최대 시도 횟수
     */
    public String markRetry(String requestId, String lastError, int maxAttempts) {
        return jdbcClient.sql(sql.markDispatchRetry())
                .param("request_id", requestId)
                .param("last_error", lastError)
                .param("max_attempts", maxAttempts)
                .query(String.class)
                .single();
    }
}
