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

import java.util.Map;

/**
 * member_llm_context 적재 결과를 최소 기준으로 검증하는 tasklet.
 *
 * 이 step의 목적은 "SQL이 실행됐다" 수준에서 멈추지 않고,
 * 실제 결과가 우리가 기대한 형태로 들어갔는지 빠르게 확인하는 데 있다.
 *
 * 현재 검증 항목은 다음과 같다.
 * 1. 적재 대상자 수와 실제 적재 row 수가 같은가
 * 2. PK(member_id)가 비어 있는 row가 없는가
 * 3. segment가 허용값(CHURN_RISK, UPSELL, NORMAL) 안에 있는가
 * 4. current_product_types가 null 없이 채워졌는가
 */
@Component
@RequiredArgsConstructor
public class VerifyMemberLlmContextTasklet implements Tasklet {

    /**
     * 검증 SQL 파일 경로.
     *
     * SQL에서 적재 대상 건수, 실제 적재 건수, null 건수, 잘못된 segment 건수를
     * 한 번에 가져와서 자바 쪽에서 판정한다.
     */
    private static final String VERIFY_SQL_PATH = "sql/member-llm-context/verify_member_llm_context.sql";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final SqlFileLoader sqlFileLoader;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        // 검증 SQL을 읽어 현재 적재 상태를 한 번에 조회한다.
        String verifySql = sqlFileLoader.load(VERIFY_SQL_PATH);
        Map<String, Object> row = namedParameterJdbcTemplate.queryForMap(verifySql, new MapSqlParameterSource());

        long eligibleCount = getLong(row, "eligible_count");
        long contextCount = getLong(row, "context_count");
        long nullPkCount = getLong(row, "null_pk_count");
        long invalidSegmentCount = getLong(row, "invalid_segment_count");
        long nullProductTypesCount = getLong(row, "null_product_types_count");

        // 현재 배치 기준 적재 대상자 수와 실제 member_llm_context row 수가 달라지면
        // 중간 누락 또는 과적재가 발생한 것이므로 바로 실패시킨다.
        if (eligibleCount != contextCount) {
            throw new IllegalStateException(String.format(
                    "member_llm_context 건수 검증에 실패했습니다. 적재 대상 수=%d, 실제 적재 수=%d",
                    eligibleCount,
                    contextCount
            ));
        }

        // PK는 절대 비면 안 된다.
        if (nullPkCount > 0) {
            throw new IllegalStateException(
                    "member_llm_context PK(member_id) null 검증에 실패했습니다. nullPkCount=" + nullPkCount
            );
        }

        // segment는 허용된 세 값만 들어가야 한다.
        if (invalidSegmentCount > 0) {
            throw new IllegalStateException(
                    "member_llm_context segment 값 검증에 실패했습니다. invalidSegmentCount=" + invalidSegmentCount
            );
        }

        // current_product_types는 후속 추천/프롬프트 구성에 바로 쓰이므로 null이면 안 된다.
        if (nullProductTypesCount > 0) {
            throw new IllegalStateException(
                    "member_llm_context current_product_types null 검증에 실패했습니다. nullProductTypesCount="
                            + nullProductTypesCount
            );
        }

        return RepeatStatus.FINISHED;
    }

    /**
     * queryForMap 결과에서 숫자 값을 long으로 안전하게 꺼낸다.
     *
     * 검증 SQL이 바뀌었거나 예상과 다른 타입이 들어오면
     * 조용히 넘어가지 않고 즉시 실패시켜 원인을 빨리 드러낸다.
     */
    private long getLong(Map<String, Object> row, String key) {
        Object value = row.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException(
                "검증 SQL 결과를 숫자로 변환하지 못했습니다. key=" + key + ", value=" + value
        );
    }
}