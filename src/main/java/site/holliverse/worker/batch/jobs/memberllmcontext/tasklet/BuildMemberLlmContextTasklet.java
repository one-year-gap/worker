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

        // SQL 내부에서 기준일과 전월 집계월을 함께 사용한다.
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshotDate", snapshotDate)
                .addValue("yyyymm", yyyymm);

        String upsertSql = sqlFileLoader.load(UPSERT_SQL_PATH);
        namedParameterJdbcTemplate.update(upsertSql, params);
        return RepeatStatus.FINISHED;
    }
}