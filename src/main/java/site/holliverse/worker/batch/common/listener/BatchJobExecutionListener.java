package site.holliverse.worker.batch.common.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;
import site.holliverse.worker.global.logging.BatchLogContext;

@Component
@Slf4j
public class BatchJobExecutionListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        BatchLogContext.putJob(jobExecution);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        try {
            log.info("job finished. status={}, exitCode={}",
                    jobExecution.getStatus(), jobExecution.getExitStatus().getExitCode());
        } finally {
            BatchLogContext.clear();
        }
    }
}
