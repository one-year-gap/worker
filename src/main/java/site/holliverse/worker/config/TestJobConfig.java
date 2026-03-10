package site.holliverse.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import site.holliverse.worker.batch.common.listener.BatchJobExecutionListener;
import site.holliverse.worker.batch.common.listener.BatchStepExecutionListener;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class TestJobConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchJobExecutionListener batchJobExecutionListener;
    private final BatchStepExecutionListener batchStepExecutionListener;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper mapper;

    @Bean("testJob")
    public Job testJob() {
        //RunTime 검증을 위한 testJob 구성
        return new JobBuilder("testJob", jobRepository)
                .listener(batchJobExecutionListener)
                .start(new StepBuilder("testStep", jobRepository)
                        .listener(batchStepExecutionListener)
                        .tasklet((contribution, chunkContext) -> {
                            //1. DB 연결 검증
                            Integer ping = jdbcTemplate.queryForObject("select 1", Integer.class);
                            if (ping == null || ping != 1) {
                                throw new IllegalStateException("DB ping failed");
                            }


                            //2. 출력 경로 검증
                            Path marker = markerPath();
                            Files.createDirectories(marker.getParent());
                            String workflowRunId = firstNonBlank(
                                    System.getenv("WORKFLOW_RUN_ID"),
                                    "local"
                            );
                            String payload = mapper.writeValueAsString(Map.of(
                                    "job", "testJob",
                                    "status", "COMPLETED",
                                    "runId", workflowRunId,
                                    "executedAt", Instant.now().toString()
                            ));
                            Files.writeString(
                                    marker,
                                    payload,
                                    StandardCharsets.UTF_8,
                                    StandardOpenOption.CREATE,
                                    StandardOpenOption.TRUNCATE_EXISTING
                            );

                            log.info("verification marker written: {}", marker);
                            contribution.setExitStatus(ExitStatus.COMPLETED);

                            return RepeatStatus.FINISHED;
                        }, transactionManager)
                        .build())
                .build();
    }

    private Path markerPath() {
        String base = firstNonBlank(
                System.getenv("WORKFLOW_OUTPUT_PATH"),
                System.getenv("WORKFLOW_OUTPUB_PATH"),
                "/tmp/holliverse-worker"
        );

        return Paths.get(base).toAbsolutePath().normalize().resolve("verify-success.json");
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        throw new IllegalStateException("marker path resolution failed");
    }
}
