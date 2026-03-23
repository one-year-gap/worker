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
public class DeleteExistingSnapshotTasklet implements Tasklet {

    private final AthenaFeatureSyncRepository repository;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 현재 구현은 staging 없이 최종 테이블에 바로 적재한다.
        // 같은 snapshotDate로 재실행될 수 있으므로 먼저 기존 데이터를 정리한다.
        String snapshotDate = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .getString(AthenaFeatureSyncContextKeys.SNAPSHOT_DATE);

        int deleted = repository.deleteBySnapshotDate(snapshotDate);
        log.info("Existing snapshot rows deleted. snapshotDate={}, deleted={}", snapshotDate, deleted);
        return RepeatStatus.FINISHED;
    }
}
