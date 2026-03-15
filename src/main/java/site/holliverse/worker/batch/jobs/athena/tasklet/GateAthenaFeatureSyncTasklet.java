package site.holliverse.worker.batch.jobs.athena.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.jobs.athena.support.AthenaFeatureSyncContextKeys;
import site.holliverse.worker.batch.jobs.athena.support.AthenaFeatureSyncProperties;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

@Component
@RequiredArgsConstructor
@Slf4j
public class GateAthenaFeatureSyncTasklet implements Tasklet {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private final JdbcTemplate jdbcTemplate;
    private final AthenaFeatureSyncProperties properties;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 외부에서 받은 snapshotDate 파라미터를 읽는다.
        // 값이 없으면 기본적으로 KST 오늘 날짜를 사용한다.
        String snapshotDateParam = (String) chunkContext.getStepContext()
                .getJobParameters()
                .get("snapshotDate");

        LocalDate snapshotDate = resolveSnapshotDate(snapshotDateParam);
        // Job 시작 전에 필수 설정과 대상 테이블 존재 여부를 확인한다.
        validateInfrastructure();
        verifyRequiredTable();

        // 다음 Step들이 공통으로 사용할 수 있게 ExecutionContext에 저장한다.
        chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .putString(AthenaFeatureSyncContextKeys.SNAPSHOT_DATE, snapshotDate.toString());

        log.info("Athena feature sync gate passed. snapshotDate={}", snapshotDate);
        return RepeatStatus.FINISHED;
    }

    private LocalDate resolveSnapshotDate(String snapshotDateParam) {
        // snapshotDate가 없으면 스케줄 배치가 바로 돌 수 있도록 오늘 날짜를 기본값으로 사용한다.
        if (snapshotDateParam == null || snapshotDateParam.isBlank()) {
            return LocalDate.now(KST);
        }

        try {
            return LocalDate.parse(snapshotDateParam);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "snapshotDate format is invalid. expected yyyy-MM-dd",
                    e
            );
        }
    }

    private void validateInfrastructure() {
        // Athena 결과 CSV 저장 경로는 필수다.
        // 이 값이 없으면 Athena는 쿼리 결과를 어디에 저장할지 결정할 수 없다.
        if (properties.getOutputLocation() == null || properties.getOutputLocation().isBlank()) {
            throw new IllegalStateException("worker.job.athena-feature-sync.output-location is required");
        }
    }

    private void verifyRequiredTable() {
        // 최종 적재 대상 테이블이 없는 상태로 Job이 시작되면 import 단계에서 실패하므로 초기에 검증한다.
        String regClass = jdbcTemplate.queryForObject(
                "select to_regclass(?)",
                String.class,
                "user_event_features_7d"
        );

        if (regClass == null) {
            throw new IllegalStateException("Required table does not exist: user_event_features_7d");
        }
    }
}
