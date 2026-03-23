package site.holliverse.worker.batch.jobs.athena.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.jobs.athena.repository.AthenaFeatureSyncRepository;
import site.holliverse.worker.batch.jobs.athena.support.AthenaFeatureSyncContextKeys;

@Component
@RequiredArgsConstructor
@Slf4j
public class VerifyUserEventFeaturesTasklet implements Tasklet {

    private final AthenaFeatureSyncRepository repository;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        var executionContext = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        // 적재가 끝난 뒤 해당 snapshotDate 데이터가 실제로 들어갔는지 최소 검증을 수행한다.
        String snapshotDate = executionContext.getString(AthenaFeatureSyncContextKeys.SNAPSHOT_DATE);
        long rowCount = repository.countBySnapshotDate(snapshotDate);

        if (rowCount <= 0) {
            throw new IllegalStateException("Imported row count is zero. snapshotDate=" + snapshotDate);
        }

        // 마지막 Step 로그에서 사용할 수 있도록 적재 건수를 남긴다.
        executionContext.putLong(AthenaFeatureSyncContextKeys.IMPORTED_ROW_COUNT, rowCount);

        log.info("Verified imported rows. snapshotDate={}, rowCount={}", snapshotDate, rowCount);
        return RepeatStatus.FINISHED;
    }
}
