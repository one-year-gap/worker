package site.holliverse.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;

@Configuration
@Getter
@Slf4j
@RequiredArgsConstructor
public class BatchJobConfiguration {
    @Value("${spring.batch.job.name:}")
    private String jobName;

    @Value("${spring.batch.testJob.startTime:}")
    private String testJobStartTime;

    private final ObjectMapper mapper;

    @PostConstruct
    public void logConfiguration(){
        log.info("========= Batch Configuration =========");
        log.info("jobName: '{}'", jobName);
        log.info("job1StartTime: '{}'", testJobStartTime);
        log.info("=======================================");
    }

    /**
     * testJob -> Instant parsing
     */
    public Instant getTestJobTimeAsInstant(){
        if (testJobStartTime ==null || testJobStartTime.isBlank()){
            throw new IllegalArgumentException("batch.testJob.startTime 값이 입력되지 않았습니다.");
        }

        return Instant.parse(testJobStartTime);
    }
}
