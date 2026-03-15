package site.holliverse.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import site.holliverse.worker.config.BatchJobConfiguration;
import site.holliverse.worker.config.TimeProperties;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

@Component
@Slf4j
public class BatchRunner implements CommandLineRunner {
    private final ApplicationContext applicationContext;
    private final JobLauncher jobLauncher;
    private final BatchJobConfiguration configuration;
    private final TimeProperties timeProperties;
    private final boolean exitOnComplete;
    private final boolean addRunId;
    private final ZoneId zoneId;
    private final DateTimeFormatter formatter;

    public BatchRunner(
            ApplicationContext applicationContext,
            JobLauncher jobLauncher,
            BatchJobConfiguration configuration,
            TimeProperties timeProperties,
            @Value("${BATCH_EXIT_ON_COMPLETE:true}") boolean exitOnComplete,
            @Value("${BATCH_ADD_RUN_ID:false}") boolean addRunId
    ) {
        this.applicationContext = applicationContext;
        this.jobLauncher = jobLauncher;
        this.configuration = configuration;
        this.timeProperties = timeProperties;
        this.exitOnComplete = exitOnComplete;
        this.addRunId = addRunId;

        this.zoneId = ZoneId.of(timeProperties.zone());
        this.formatter = DateTimeFormatter.ofPattern(timeProperties.format());
    }

    @Override
    public void run(String... args) throws Exception {
        String jobName = configuration.getJobName();

        //jobName 검증
        if (jobName == null || jobName.isBlank()) {
            throw new IllegalArgumentException("spring.batch.job.name 값이 필요.");
        }

        Job job = applicationContext.getBean(jobName, Job.class);

        JobParametersBuilder params = new JobParametersBuilder()
                .addString("requestedAt", ZonedDateTime.now(zoneId).format(formatter));


        if ("testJob".equals(jobName)) {
            params.addString("testStartTime", configuration.resolveTestStartTime());
        }

        String snapshotDate = configuration.resolveSnapshotDate();
        if (snapshotDate != null) {
            params.addString("snapshotDate", snapshotDate);
        }

        if (addRunId) {
            params.addLong("run.id", System.currentTimeMillis());
        }

        JobExecution execution = jobLauncher.run(job, params.toJobParameters());

        BatchStatus status = execution.getStatus();
        String exitCode = execution.getExitStatus().getExitCode();

        //COMPLETED 아니면 예외
        boolean completed = status == BatchStatus.COMPLETED
                            && ExitStatus.COMPLETED.getExitCode().equals(exitCode);

        if (!completed) {
            throw new IllegalStateException(
                    "Batch verification failed: status=" + status + ", exitCode=" + exitCode
            );
        }
    }
}
