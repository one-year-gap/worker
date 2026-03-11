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

import java.util.Map;

/**
 * 적재 결과를 최종 검증하는 Step.
 *
 * 검증 항목:
 * - member 수 == raw 수 == tscore 수 == persona 수
 * - tscore 필수 컬럼 null 여부
 * - persona 필수 컬럼 null 여부
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class VerifyWeeklyIndexTasklet implements Tasklet {

    private static final String VERIFY_COUNTS_SQL_PATH = "sql/index/verify_counts.sql";
    private static final String VERIFY_TSCORE_NULLS_SQL_PATH = "sql/index/verify_tscore_nulls.sql";
    private static final String VERIFY_PERSONA_NULLS_SQL_PATH = "sql/index/verify_persona_nulls.sql";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final SqlFileLoader sqlFileLoader;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 1) 검증 대상 snapshotDate를 읽는다.
        String snapshotDate = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .getString("snapshotDate");

        // 2) SQL 파라미터를 구성한다.
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshotDate", snapshotDate);

        // 3) 검증 SQL 파일을 로드한다.
        String verifyCountsSql = sqlFileLoader.load(VERIFY_COUNTS_SQL_PATH);
        String verifyTScoreNullsSql = sqlFileLoader.load(VERIFY_TSCORE_NULLS_SQL_PATH);
        String verifyPersonaNullsSql = sqlFileLoader.load(VERIFY_PERSONA_NULLS_SQL_PATH);

        // 4) 건수 검증: 모집단 대비 누락/중복 여부를 확인한다.
        Map<String, Object> counts = namedParameterJdbcTemplate.queryForMap(verifyCountsSql, params);
        long memberCount = getLong(counts, "member_count");
        long rawCount = getLong(counts, "raw_count");
        long tscoreCount = getLong(counts, "tscore_count");
        long personaCount = getLong(counts, "persona_count");

        if (memberCount != rawCount || rawCount != tscoreCount || tscoreCount != personaCount) {
            throw new IllegalStateException(String.format(
                    "건수 검증에 실패했습니다. member=%d, raw=%d, tscore=%d, persona=%d",
                    memberCount, rawCount, tscoreCount, personaCount
            ));
        }

        // 5) tscore null 검증
        Long tscoreNullCount = namedParameterJdbcTemplate.queryForObject(verifyTScoreNullsSql, params, Long.class);
        if (tscoreNullCount != null && tscoreNullCount > 0) {
            throw new IllegalStateException("T-score null 검증에 실패했습니다. nullCount=" + tscoreNullCount);
        }

        // 6) persona null 검증
        Long personaNullCount = namedParameterJdbcTemplate.queryForObject(verifyPersonaNullsSql, params, Long.class);
        if (personaNullCount != null && personaNullCount > 0) {
            throw new IllegalStateException("Persona null 검증에 실패했습니다. nullCount=" + personaNullCount);
        }

        log.info(
                "Verify 완료. snapshotDate={}, memberCount={}, rawCount={}, tscoreCount={}, personaCount={}, tscoreNullCount={}, personaNullCount={}",
                snapshotDate,
                memberCount,
                rawCount,
                tscoreCount,
                personaCount,
                tscoreNullCount,
                personaNullCount
        );

        return RepeatStatus.FINISHED;
    }

    private long getLong(Map<String, Object> row, String key) {
        Object value = row.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException("숫자 값 변환에 실패했습니다. key=" + key + ", value=" + value);
    }
}
