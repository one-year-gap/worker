package site.holliverse.worker.batch.jobs.athena.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.jobs.athena.service.AthenaQueryService;
import site.holliverse.worker.batch.jobs.athena.support.AthenaFeatureSyncContextKeys;
import site.holliverse.worker.batch.jobs.athena.support.AthenaSqlTemplateFactory;

@Component
@RequiredArgsConstructor
@Slf4j
public class StartAthenaQueryTasklet implements Tasklet {

    private final AthenaQueryService athenaQueryService;
    private final AthenaSqlTemplateFactory athenaSqlTemplateFactory;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        var executionContext = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        // Gate Step에서 확정한 snapshotDate를 기준으로 Athena SQL을 만든다.
        String snapshotDate = executionContext.getString(AthenaFeatureSyncContextKeys.SNAPSHOT_DATE);
        String sql = athenaSqlTemplateFactory.buildUserEventFeaturesQuery(snapshotDate);

        // Athena에 쿼리를 제출하면 즉시 queryExecutionId를 받는다.
        // 실제 결과 파일 경로는 아직 모르고, 다음 Step에서 완료 polling 후 획득한다.
        String queryExecutionId = athenaQueryService.startQueryExecution(sql);

        executionContext.putString(AthenaFeatureSyncContextKeys.QUERY_EXECUTION_ID, queryExecutionId);

        log.info("Athena query submitted. snapshotDate={}, queryExecutionId={}", snapshotDate, queryExecutionId);
        return RepeatStatus.FINISHED;
    }
}
