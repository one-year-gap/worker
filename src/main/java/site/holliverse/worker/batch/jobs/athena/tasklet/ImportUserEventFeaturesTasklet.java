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
public class ImportUserEventFeaturesTasklet implements Tasklet {

    private final AthenaFeatureSyncRepository repository;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        var executionContext = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        // Athena가 실제로 생성한 결과 CSV의 bucket/key를 사용한다.
        // 파일명을 추측하지 않고 OutputLocation을 그대로 신뢰하는 방식이다.
        String bucket = executionContext.getString(AthenaFeatureSyncContextKeys.OUTPUT_BUCKET);
        String key = executionContext.getString(AthenaFeatureSyncContextKeys.OUTPUT_KEY);

        // PostgreSQL aws_s3 확장을 통해 RDS가 S3에서 직접 CSV를 읽는다.
        // 배치 애플리케이션이 파일 내용을 중간에서 내려받아 전달하지 않는다.
        repository.importFromS3(bucket, key);

        log.info("Imported Athena output into user_event_features_7d. bucket={}, key={}", bucket, key);
        return RepeatStatus.FINISHED;
    }
}
