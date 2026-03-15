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
import site.holliverse.worker.batch.jobs.athena.support.S3OutputLocationParser;

@Component
@RequiredArgsConstructor
@Slf4j
public class WaitAthenaCompletionTasklet implements Tasklet {

    private final AthenaQueryService athenaQueryService;
    private final S3OutputLocationParser s3OutputLocationParser;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        var executionContext = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        // 이전 Step에서 저장한 queryExecutionId로 Athena 실행 상태를 조회한다.
        String queryExecutionId = executionContext.getString(AthenaFeatureSyncContextKeys.QUERY_EXECUTION_ID);

        // Athena가 성공하면 결과 CSV의 전체 S3 경로를 반환한다.
        String outputLocation = athenaQueryService.waitForSucceeded(queryExecutionId);

        // 이후 RDS import에서 bucket/key가 필요하므로 S3 URI를 분해해 함께 저장한다.
        S3OutputLocationParser.ParsedS3Path parsed = s3OutputLocationParser.parse(outputLocation);

        executionContext.putString(AthenaFeatureSyncContextKeys.OUTPUT_LOCATION, outputLocation);
        executionContext.putString(AthenaFeatureSyncContextKeys.OUTPUT_BUCKET, parsed.bucket());
        executionContext.putString(AthenaFeatureSyncContextKeys.OUTPUT_KEY, parsed.key());

        log.info(
                "Athena query succeeded. queryExecutionId={}, outputLocation={}",
                queryExecutionId,
                outputLocation
        );
        return RepeatStatus.FINISHED;
    }
}
