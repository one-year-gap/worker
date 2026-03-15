package site.holliverse.worker.batch.jobs.athena;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import site.holliverse.worker.batch.jobs.athena.tasklet.CleanupAthenaFeatureSyncTasklet;
import site.holliverse.worker.batch.jobs.athena.tasklet.DeleteExistingSnapshotTasklet;
import site.holliverse.worker.batch.jobs.athena.tasklet.GateAthenaFeatureSyncTasklet;
import site.holliverse.worker.batch.jobs.athena.tasklet.ImportUserEventFeaturesTasklet;
import site.holliverse.worker.batch.jobs.athena.tasklet.StartAthenaQueryTasklet;
import site.holliverse.worker.batch.jobs.athena.tasklet.VerifyUserEventFeaturesTasklet;
import site.holliverse.worker.batch.jobs.athena.tasklet.WaitAthenaCompletionTasklet;

@Configuration
@RequiredArgsConstructor
public class AthenaFeatureSyncJobConfig {

    public static final String JOB_NAME = "athenaFeatureSyncJob";

    /**
     * Athena feature 동기화 Job.
     *
     * 처리 흐름:
     * 1) snapshotDate 확정
     * 2) Athena 쿼리 실행
     * 3) Athena 완료 대기 및 결과 S3 경로 획득
     * 4) 동일 snapshotDate 기존 데이터 삭제
     * 5) Athena 결과 CSV를 최종 테이블로 import
     * 6) 적재 결과 검증
     * 7) 실행 메타 로그 마무리
     */
    @Bean
    public Job athenaFeatureSyncJob(
            JobRepository jobRepository,
            Step athenaFeatureSyncGateStep,
            Step startAthenaQueryStep,
            Step waitAthenaCompletionStep,
            Step deleteExistingSnapshotStep,
            Step importUserEventFeaturesStep,
            Step verifyUserEventFeaturesStep,
            Step cleanupAthenaFeatureSyncStep
    ) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(athenaFeatureSyncGateStep)
                .next(startAthenaQueryStep)
                .next(waitAthenaCompletionStep)
                .next(deleteExistingSnapshotStep)
                .next(importUserEventFeaturesStep)
                .next(verifyUserEventFeaturesStep)
                .next(cleanupAthenaFeatureSyncStep)
                .build();
    }

    @Bean
    public Step athenaFeatureSyncGateStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            GateAthenaFeatureSyncTasklet tasklet
    ) {
        return new StepBuilder("Step00_Gate", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step startAthenaQueryStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            StartAthenaQueryTasklet tasklet
    ) {
        return new StepBuilder("Step01_StartAthenaQuery", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step waitAthenaCompletionStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            WaitAthenaCompletionTasklet tasklet
    ) {
        return new StepBuilder("Step02_WaitAthenaCompletion", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step deleteExistingSnapshotStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            DeleteExistingSnapshotTasklet tasklet
    ) {
        return new StepBuilder("Step03_DeleteExistingSnapshot", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step importUserEventFeaturesStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            ImportUserEventFeaturesTasklet tasklet
    ) {
        return new StepBuilder("Step04_ImportUserEventFeatures", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step verifyUserEventFeaturesStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            VerifyUserEventFeaturesTasklet tasklet
    ) {
        return new StepBuilder("Step05_VerifyUserEventFeatures", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step cleanupAthenaFeatureSyncStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            CleanupAthenaFeatureSyncTasklet tasklet
    ) {
        return new StepBuilder("Step06_CleanupAthenaFeatureSync", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }
}
