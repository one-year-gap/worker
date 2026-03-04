package site.holliverse.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import site.holliverse.worker.config.BatchJobConfiguration;
import site.holliverse.worker.config.TimeProperties;
import java.time.ZoneId;
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
        log.info("Resolved spring.batch.job.name='{}'", configuration.getJobName());

        switch (jobName) {
            case "testJob":
                log.info("TestJob");
                break;
            default:
                throw new IllegalArgumentException("unknown job name!");
        }
    }
}
