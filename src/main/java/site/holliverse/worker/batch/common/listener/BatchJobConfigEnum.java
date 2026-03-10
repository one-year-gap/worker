package site.holliverse.worker.batch.common.listener;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum BatchJobConfigEnum {
    KEY_JOB_NAME("jobName"),
    KEY_JOB_INSTANCE("jobInstanceId"),
    KEY_STEP_NAME("stepName");

    private final String key;
}
