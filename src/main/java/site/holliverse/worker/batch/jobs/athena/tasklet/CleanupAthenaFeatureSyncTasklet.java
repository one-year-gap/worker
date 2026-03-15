package site.holliverse.worker.batch.jobs.athena.tasklet;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.jobs.athena.support.AthenaFeatureSyncContextKeys;

@Component
@Slf4j
public class CleanupAthenaFeatureSyncTasklet implements Tasklet {

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 현재는 별도 정리 작업보다는 실행 메타 정보를 한 번에 남기는 역할만 수행한다.
        // 나중에 Athena 실행 로그 테이블이나 알림 연동이 필요하면 이 Step을 확장하면 된다.
        var executionContext = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        log.info(
                "Athena feature sync completed. snapshotDate={}, queryExecutionId={}, outputLocation={}, importedRowCount={}",
                executionContext.getString(AthenaFeatureSyncContextKeys.SNAPSHOT_DATE),
                executionContext.getString(AthenaFeatureSyncContextKeys.QUERY_EXECUTION_ID),
                executionContext.getString(AthenaFeatureSyncContextKeys.OUTPUT_LOCATION),
                executionContext.getLong(AthenaFeatureSyncContextKeys.IMPORTED_ROW_COUNT, 0L)
        );
        return RepeatStatus.FINISHED;
    }
}
