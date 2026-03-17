package site.holliverse.worker.batch.jobs.memberllmcontext.tasklet;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.common.support.SqlFileLoader;

/**
 * member_llm_context upsert SQL을 실제로 실행하는 tasklet.
 *
 * 설계상 이 step은 reader/processor/writer가 아니라,
 * SQL 한 번으로 전체 적재를 끝내는 set-based 배치다.
 */
@Component
@RequiredArgsConstructor
public class BuildMemberLlmContextTasklet implements Tasklet {

    private static final String UPSERT_SQL_PATH = "sql/member-llm-context/upsert_member_llm_context.sql";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final SqlFileLoader sqlFileLoader;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        String snapshotDate = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .getString("snapshotDate");

        String yyyymm = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .getString("yyyymm");

        /**
         * SQL 내부에서 기준일과 전월 집계월을 함께 사용한다.
         * snapshotDate는 나이/활성 구독/약정/로그 범위 계산에 쓰이고,
         * yyyymm은 usage_monthly 전월 데이터 조회에 쓰인다.
         */
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshotDate", snapshotDate)
                .addValue("yyyymm", yyyymm);

        /**
         * 예외 처리 이유:
         * - SQL 파일을 읽지 못하면 실제 적재 로직 자체가 없다는 뜻이므로 바로 실패해야 한다.
         * - SQL 실행 중 예외가 나면 조인 대상 테이블, 타입, 데이터 형식, 제약조건 중 하나가 어긋난 상황일 가능성이 크다.
         * - 이 step은 핵심 적재 단계이므로 예외를 삼키지 않고 상위로 그대로 올려 배치를 실패시키는 편이 맞다.
         */
        String upsertSql = sqlFileLoader.load(UPSERT_SQL_PATH);
        namedParameterJdbcTemplate.update(upsertSql, params);
        return RepeatStatus.FINISHED;
    }
}