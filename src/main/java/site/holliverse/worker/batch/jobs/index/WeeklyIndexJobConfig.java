package site.holliverse.worker.batch.jobs.index;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import site.holliverse.worker.batch.jobs.index.tasklet.BuildPersonaWeeklyIndexTasklet;
import site.holliverse.worker.batch.jobs.index.tasklet.BuildRawWeeklyIndexTasklet;
import site.holliverse.worker.batch.jobs.index.tasklet.BuildTScoreWeeklyIndexTasklet;
import site.holliverse.worker.batch.jobs.index.tasklet.GateWeeklyIndexTasklet;
import site.holliverse.worker.batch.jobs.index.tasklet.VerifyWeeklyIndexTasklet;

@Configuration
@RequiredArgsConstructor
public class WeeklyIndexJobConfig {

    public static final String JOB_NAME = "weeklyIndexJob";

    @Bean
    public Job weeklyIndexJob(
            JobRepository jobRepository,
            Step gateStep,
            Step buildRawStep,
            Step buildTScoreStep,
            Step buildPersonaStep,
            Step verifyStep
    ) {
        // 주간 지수 배치 플로우: 준비 -> raw -> tscore -> persona -> 검증
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(gateStep)
                .next(buildRawStep)
                .next(buildTScoreStep)
                .next(buildPersonaStep)
                .next(verifyStep)
                .build();
    }

    @Bean
    public Step gateStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            GateWeeklyIndexTasklet tasklet
    ) {
        return new StepBuilder("Step00_Gate", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step buildRawStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            BuildRawWeeklyIndexTasklet tasklet
    ) {
        return new StepBuilder("Step01_BuildRaw", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step buildTScoreStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            BuildTScoreWeeklyIndexTasklet tasklet
    ) {
        return new StepBuilder("Step02_BuildTScore", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step buildPersonaStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            BuildPersonaWeeklyIndexTasklet tasklet
    ) {
        return new StepBuilder("Step03_BuildPersona", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    @Bean
    public Step verifyStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            VerifyWeeklyIndexTasklet tasklet
    ) {
        return new StepBuilder("Step04_Verify", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }
}
