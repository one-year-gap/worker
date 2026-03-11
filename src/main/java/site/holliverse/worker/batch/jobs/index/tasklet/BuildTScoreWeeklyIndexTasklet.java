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
 * T-score 스냅샷을 생성하는 Step.
 *
 * 처리 방식:
 * - 동일 snapshotDate 기존 tscore 결과 삭제
 * - raw 분포(avg/stddev) 기반으로 tscore 재계산 후 적재
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class BuildTScoreWeeklyIndexTasklet implements Tasklet {

    private static final String DELETE_SQL_PATH = "sql/index/delete_tscore_snapshot.sql";
    private static final String INSERT_SQL_PATH = "sql/index/insert_tscore_snapshot.sql";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final SqlFileLoader sqlFileLoader;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 1) 현재 스냅샷 날짜를 컨텍스트에서 읽는다.
        String snapshotDate = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .getString("snapshotDate");

        // 2) SQL 파라미터를 구성한다.
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshotDate", snapshotDate);

        // 3) SQL 파일을 로드한다.
        String deleteSql = sqlFileLoader.load(DELETE_SQL_PATH);
        String insertSql = sqlFileLoader.load(INSERT_SQL_PATH);

        // 4) 기존 결과를 초기화하고 새 결과를 적재한다.
        int deleted = namedParameterJdbcTemplate.update(deleteSql, params);
        int inserted = namedParameterJdbcTemplate.update(insertSql, params);

        log.info("BuildTScore 완료. snapshotDate={}, deleted={}, inserted={}",
                snapshotDate, deleted, inserted);

        return RepeatStatus.FINISHED;
    }
}
