package site.holliverse.worker.batch.consultation.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import site.holliverse.worker.batch.common.listener.BatchJobExecutionListener;
import site.holliverse.worker.batch.common.listener.BatchStepExecutionListener;
import site.holliverse.worker.batch.consultation.step.ClaimStepTasklet;
import site.holliverse.worker.batch.consultation.step.DispatchTasklet;

@Configuration
@RequiredArgsConstructor
public class ConsultationKeywordAnalysisJobConfig {
    private static final String JOB_NAME = "consultationKeywordAnalysisJob";
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchJobExecutionListener batchJobExecutionListener;
    private final BatchStepExecutionListener batchStepExecutionListener;
    private final ClaimStepTasklet claimStepTasklet;
    private final DispatchTasklet dispatchTasklet;

    @Bean("consultation-keyword-analysis-job")
    public Job consultationKeywordAnalysisJob(){
        return new JobBuilder(JOB_NAME,jobRepository)
                .listener(batchJobExecutionListener)
                .start(claimStep())
                .on(ClaimStepTasklet.EXIT_NO_CLAIMED_CASES).end()
                .from(claimStep()).on(ExitStatus.COMPLETED.getExitCode()).to(dispatchStep())
                .from(claimStep()).on("*").fail()
                .from(dispatchStep()).on(ExitStatus.COMPLETED.getExitCode()).to(claimStep())
                .from(dispatchStep()).on("*").fail()
                .end()
                .build();
    }

    @Bean
    public Step claimStep(){
        return new StepBuilder("claimStep",jobRepository)
                .listener(batchStepExecutionListener)
                .tasklet(claimStepTasklet,transactionManager)
                .build();
    }

    @Bean
    public Step dispatchStep(){
        return new StepBuilder("dispatchStep",jobRepository)
                .listener(batchStepExecutionListener)
                .tasklet(dispatchTasklet,transactionManager)
                .build();
    }
}
