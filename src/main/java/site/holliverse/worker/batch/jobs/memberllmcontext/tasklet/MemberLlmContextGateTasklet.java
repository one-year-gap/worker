package site.holliverse.worker.batch.jobs.memberllmcontext.tasklet;

import lombok.RequiredArgsConstructor;
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
import java.util.ArrayList;
import java.util.List;

/**
 * member_llm_context 배치 실행 전 공통 값을 준비하는 tasklet.
 *
 * 역할:
 * - snapshotDate 파라미터를 읽고 기본값을 보정한다.
 * - usage_monthly 조회에 쓸 전월 yyyymm을 계산한다.
 * - 실제 적재에 필요한 필수 테이블이 모두 있는지 확인한다.
 */
@Component
@RequiredArgsConstructor
public class MemberLlmContextGateTasklet implements Tasklet {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter YYYYMM = DateTimeFormatter.ofPattern("yyyyMM");

    /**
     * 현재 잡이 정상적으로 동작하려면 반드시 필요한 테이블 목록.
     * churn 스냅샷이 생긴 이후에는 해당 테이블도 필수로 본다.
     */
    private static final List<String> REQUIRED_TABLES = List.of(
            "member",
            "subscription",
            "product",
            "mobile_plan",
            "support_case",
            "usage_monthly",
            "user_event_features_7d",
            "index_persona_snapshot",
            "index_tscore_snapshot",
            "churn_score_snapshot",
            "member_llm_context"
    );

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        String snapshotDateParam = (String) chunkContext.getStepContext()
                .getJobParameters()
                .get("snapshotDate");

        LocalDate snapshotDate = resolveSnapshotDate(snapshotDateParam);
        String yyyymm = snapshotDate.minusMonths(1).format(YYYYMM);

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

        verifyRequiredTables();
        return RepeatStatus.FINISHED;
    }

    /**
     * snapshotDate를 yyyy-MM-dd 형식으로 파싱한다.
     * 값이 없으면 한국 시간 기준 오늘 날짜를 사용한다.
     *
     * 예외 처리 이유:
     * - 배치 기준일이 잘못되면 이후 usage, 로그 14일 범위, 약정 계산이 전부 틀어진다.
     * - 그래서 형식이 맞지 않으면 조용히 보정하지 않고 즉시 실패시킨다.
     */
    private LocalDate resolveSnapshotDate(String snapshotDateParam) {
        if (snapshotDateParam == null || snapshotDateParam.isBlank()) {
            return LocalDate.now(KST);
        }

        try {
            return LocalDate.parse(snapshotDateParam);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "snapshotDate 파라미터 형식이 올바르지 않습니다. yyyy-MM-dd 형식을 사용하세요.",
                    e
            );
        }
    }

    /**
     * 필수 테이블이 하나라도 빠져 있으면 즉시 실패시키되,
     * 운영에서 한 번에 원인을 볼 수 있도록 누락 목록 전체를 함께 보여준다.
     *
     * 예외 처리 이유:
     * - 이 잡은 여러 테이블을 동시에 조인하므로 필수 테이블 하나만 없어도 중간 step에서 실패한다.
     * - Upsert step까지 갔다가 SQL 오류로 터지는 것보다 Gate 단계에서 빠르게 실패시키는 편이 원인 파악이 쉽다.
     */
    private void verifyRequiredTables() {
        List<String> missingTables = new ArrayList<>();
        for (String table : REQUIRED_TABLES) {
            String regClass = jdbcTemplate.queryForObject("select to_regclass(?)", String.class, table);
            if (regClass == null) {
                missingTables.add(table);
            }
        }

        if (!missingTables.isEmpty()) {
            throw new IllegalStateException(
                    "member_llm_context 배치 실행에 필요한 테이블이 없습니다: " + String.join(", ", missingTables)
            );
        }
    }
}