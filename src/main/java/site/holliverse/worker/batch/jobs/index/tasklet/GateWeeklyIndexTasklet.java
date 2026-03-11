package site.holliverse.worker.batch.jobs.index.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * 주간 지수 배치의 게이트 Step.
 *
 * 역할:
 * - snapshotDate 파라미터를 읽고 유효성을 검증한다.
 * - 월 기준 키(yyyymm)를 계산한다.
 * - 후속 Step이 공통으로 쓰는 값을 ExecutionContext에 저장한다.
 * - 필수 소스/타깃 테이블 존재 여부를 사전 검증한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class GateWeeklyIndexTasklet implements Tasklet {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter YYYYMM = DateTimeFormatter.ofPattern("yyyyMM");

    // 배치 시작 전에 반드시 존재해야 하는 테이블 목록
    private static final List<String> REQUIRED_TABLES = List.of(
            "member",
            "subscription",
            "product",
            "addon_service",
            "internet",
            "iptv",
            "mobile_plan",
            "tab_watch_plan",
            "member_coupon",
            "usage_monthly",
            "support_case",
            "billing",
            "user_event_features_7d",
            "index_raw_snapshot",
            "index_tscore_snapshot",
            "index_persona_snapshot"
    );

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 1) snapshotDate 파라미터를 읽는다.
        String snapshotDateParam = (String) chunkContext.getStepContext()
                .getJobParameters()
                .get("snapshotDate");

        // 2) 기준 날짜와 월 키를 계산한다.
        LocalDate snapshotDate = resolveSnapshotDate(snapshotDateParam);
        // 월 기준 지표(usage/billing/support)는 전월 확정치를 사용한다.
        String yyyymm = snapshotDate.minusMonths(1).format(YYYYMM);

        // 3) 후속 Step에서 재사용하도록 컨텍스트에 저장한다.
        chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .putString("snapshotDate", snapshotDate.toString());

        chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .putString("yyyymm", yyyymm);

        // 4) 필수 테이블 존재 여부를 확인한다.
        verifyRequiredTables();

        log.info("Gate 통과. snapshotDate={}, yyyymm={}", snapshotDate, yyyymm);
        return RepeatStatus.FINISHED;
    }

    private LocalDate resolveSnapshotDate(String snapshotDateParam) {
        // 파라미터가 없으면 KST 오늘 날짜를 기본값으로 사용한다.
        if (snapshotDateParam == null || snapshotDateParam.isBlank()) {
            return LocalDate.now(KST);
        }

        // yyyy-MM-dd 형식만 허용한다.
        try {
            return LocalDate.parse(snapshotDateParam);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("snapshotDate 형식이 올바르지 않습니다. yyyy-MM-dd 형식을 사용하세요.", e);
        }
    }

    private void verifyRequiredTables() {
        // to_regclass는 테이블이 없으면 null을 반환한다.
        for (String table : REQUIRED_TABLES) {
            String regClass = jdbcTemplate.queryForObject(
                    "select to_regclass(?)",
                    String.class,
                    table
            );

            if (regClass == null) {
                throw new IllegalStateException("필수 테이블이 존재하지 않습니다: " + table);
            }
        }
    }
}
