package site.holliverse.worker.batch.jobs.index.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.common.support.SqlFileLoader;

/**
 * raw 지수 스냅샷을 생성하는 Step.
 *
 * 처리 방식:
 * - 동일 snapshotDate 기존 결과 삭제
 * - 같은 기준으로 raw 결과 전체 재적재
 *
 * 목적:
 * - 재실행 시에도 결과를 안정적으로 덮어써 멱등성을 보장한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class BuildRawWeeklyIndexTasklet implements Tasklet {

    private static final String DELETE_SQL_PATH = "sql/index/delete_raw_snapshot.sql";
    private static final String INSERT_SQL_PATH = "sql/index/insert_raw_snapshot.sql";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final SqlFileLoader sqlFileLoader;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 1) Gate Step에서 저장한 기준 값을 읽는다.
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

        // 2) SQL 파라미터를 구성한다.
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshotDate", snapshotDate)
                .addValue("yyyymm", yyyymm);

        // 3) 클래스패스 SQL 파일을 읽는다.
        String deleteSql = sqlFileLoader.load(DELETE_SQL_PATH);
        String insertSql = sqlFileLoader.load(INSERT_SQL_PATH);

        // 4) 기존 데이터 삭제 후 재계산 결과를 적재한다.
        int deleted = namedParameterJdbcTemplate.update(deleteSql, params);
        int inserted = namedParameterJdbcTemplate.update(insertSql, params);

        log.info("BuildRaw 완료. snapshotDate={}, yyyymm={}, deleted={}, inserted={}",
                snapshotDate, yyyymm, deleted, inserted);

        return RepeatStatus.FINISHED;
    }
}
