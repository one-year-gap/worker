package site.holliverse.worker.global.logging;

import org.slf4j.MDC;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import static site.holliverse.worker.batch.common.listener.BatchJobConfigEnum.*;

/**
 * Spring Batch 실행 컨텍스트를 MDC에 세팅/해제
 */
public final class BatchLogContext {


    private BatchLogContext() {
    }

    /**
     * Job 시작 시 호출
     */
    public static void putJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        Long jobInstanceId = jobExecution.getJobInstance().getInstanceId();

        put(KEY_JOB_NAME.getKey(), jobName);
        put(KEY_JOB_INSTANCE.getKey(), jobInstanceId == null ? null : String.valueOf(jobInstanceId));
    }

    /**
     * Step 시작 시 호출
     */
    public static void putStep(StepExecution stepExecution) {
        put(KEY_STEP_NAME.getKey(), stepExecution.getStepName());
    }

    /**
     * Step 종료 시 호출
     */
    public static void removeStep() {
        MDC.remove(KEY_STEP_NAME.getKey());
    }

    /**
     * Job 종료 시 호출
     */
    public static void clear() {
        MDC.remove(KEY_JOB_NAME.getKey());
        MDC.remove(KEY_JOB_INSTANCE.getKey());
        MDC.remove(KEY_STEP_NAME.getKey());
    }

    private static void put(String key, String value) {
        if (value == null || value.isBlank()) {
            MDC.remove(key);
            return;
        }
        MDC.put(key, value);
    }
}
