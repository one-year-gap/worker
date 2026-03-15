package site.holliverse.worker.batch.jobs.athena.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import site.holliverse.worker.batch.jobs.athena.support.AthenaFeatureSyncProperties;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

@Service
@RequiredArgsConstructor
public class AthenaQueryService {

    private final AthenaClient athenaClient;
    private final AthenaFeatureSyncProperties properties;

    /**
     * Athena 쿼리를 제출하고 queryExecutionId를 반환한다.
     *
     * 결과 저장 경로는 Athena 콘솔 기본값에 의존하지 않고
     * 배치 설정의 outputLocation을 명시적으로 넣는다.
     */
    public String startQueryExecution(String sql) {
        StartQueryExecutionRequest.Builder requestBuilder = StartQueryExecutionRequest.builder()
                .queryString(sql)
                .queryExecutionContext(QueryExecutionContext.builder()
                        .database(properties.getDatabase())
                        .build())
                .resultConfiguration(ResultConfiguration.builder()
                        .outputLocation(properties.getOutputLocation())
                        .build());

        if (properties.getWorkgroup() != null && !properties.getWorkgroup().isBlank()) {
            requestBuilder.workGroup(properties.getWorkgroup());
        }

        StartQueryExecutionResponse response = athenaClient.startQueryExecution(requestBuilder.build());
        return response.queryExecutionId();
    }

    /**
     * Athena 실행이 끝날 때까지 polling 한다.
     *
     * 성공 시 Athena가 실제로 생성한 결과 CSV의 S3 경로를 반환한다.
     * 실패나 취소 상태는 즉시 예외로 처리한다.
     */
    public String waitForSucceeded(String queryExecutionId) {
        for (int i = 0; i < properties.getMaxPollCount(); i++) {
            GetQueryExecutionResponse response = athenaClient.getQueryExecution(
                    GetQueryExecutionRequest.builder()
                            .queryExecutionId(queryExecutionId)
                            .build()
            );

            var execution = response.queryExecution();
            QueryExecutionState state = execution.status().state();

            if (state == QueryExecutionState.SUCCEEDED) {
                return execution.resultConfiguration().outputLocation();
            }

            if (state == QueryExecutionState.FAILED) {
                throw new IllegalStateException(
                        "Athena query failed. queryExecutionId=%s, reason=%s"
                                .formatted(queryExecutionId, execution.status().stateChangeReason())
                );
            }

            if (state == QueryExecutionState.CANCELLED) {
                throw new IllegalStateException(
                        "Athena query cancelled. queryExecutionId=%s".formatted(queryExecutionId)
                );
            }

            if (state != QueryExecutionState.QUEUED && state != QueryExecutionState.RUNNING) {
                throw new IllegalStateException(
                        "Unexpected Athena state. queryExecutionId=%s, state=%s"
                                .formatted(queryExecutionId, state)
                );
            }

            sleep(properties.getPollIntervalMillis());
        }

        throw new IllegalStateException(
                "Athena query polling timeout. queryExecutionId=%s".formatted(queryExecutionId)
        );
    }

    private void sleep(long millis) {
        // Athena 완료 polling은 짧은 간격으로 반복되므로 인터럽트 상태를 복구해주는 게 중요하다.
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted while polling Athena", e);
        }
    }
}
