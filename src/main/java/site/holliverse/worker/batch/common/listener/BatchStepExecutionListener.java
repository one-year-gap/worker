package site.holliverse.worker.batch.common.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;
import site.holliverse.worker.global.logging.BatchLogContext;

@Component
@Slf4j
public class BatchStepExecutionListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        BatchLogContext.putStep(stepExecution);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        try {
            log.info("step finished. status={}, readCount={}, writeCount={}, skipCount={}",
                    stepExecution.getStatus(),
                    stepExecution.getReadCount(),
                    stepExecution.getWriteCount(),
                    stepExecution.getSkipCount());
            return stepExecution.getExitStatus();
        } finally {
            BatchLogContext.removeStep();
        }
    }
}
