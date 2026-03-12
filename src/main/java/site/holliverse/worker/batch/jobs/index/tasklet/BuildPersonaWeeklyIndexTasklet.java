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
 * 페르소나 스냅샷을 생성하는 Step.
 *
 * 처리 순서:
 * 1) 동일 snapshotDate의 기존 persona 결과를 삭제
 * 2) index_tscore_snapshot에서 회원별 최고 T-score 지수 1개 선택
 * 3) 지수 코드 -> persona_code 매핑 후 index_persona_snapshot에 저장
 *
 * 동점 규칙:
 * - SQL 내부 정렬 기준(Order By)에 따라 해소
 * - 현재는 index_code 오름차순(사전순) 우선
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class BuildPersonaWeeklyIndexTasklet implements Tasklet {

    private static final String DELETE_SQL_PATH = "sql/index/delete_persona_snapshot.sql";
    private static final String INSERT_SQL_PATH = "sql/index/insert_persona_snapshot.sql";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final SqlFileLoader sqlFileLoader;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 1) Gate Step에서 확정한 snapshotDate를 컨텍스트에서 읽는다.
        String snapshotDate = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .getString("snapshotDate");

        // 2) SQL 바인딩 파라미터를 준비한다.
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshotDate", snapshotDate);

        // 3) 실행할 SQL 파일(삭제/적재)을 로드한다.
        String deleteSql = sqlFileLoader.load(DELETE_SQL_PATH);
        String insertSql = sqlFileLoader.load(INSERT_SQL_PATH);

        // 4) 멱등 실행을 위해 기존 데이터 삭제 후 재적재한다.
        int deleted = namedParameterJdbcTemplate.update(deleteSql, params);
        int inserted = namedParameterJdbcTemplate.update(insertSql, params);

        /*log.info("BuildPersona 완료. snapshotDate={}, deleted={}, inserted={}",
                snapshotDate, deleted, inserted);*/

        return RepeatStatus.FINISHED;
    }
}
