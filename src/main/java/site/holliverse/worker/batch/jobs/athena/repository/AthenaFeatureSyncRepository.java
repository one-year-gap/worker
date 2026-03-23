package site.holliverse.worker.batch.jobs.athena.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import site.holliverse.worker.batch.common.support.SqlFileLoader;
import site.holliverse.worker.batch.jobs.athena.support.AthenaFeatureSyncProperties;

@Repository
@RequiredArgsConstructor
public class AthenaFeatureSyncRepository {

    private static final String DELETE_SQL_PATH = "sql/athena/delete_user_event_features_7d_snapshot.sql";
    private static final String VERIFY_SQL_PATH = "sql/athena/verify_user_event_features_7d.sql";

    private static final String TARGET_TABLE = "user_event_features_7d";
    private static final String TARGET_COLUMNS = """
            snapshot_date,
            member_id,
            click_list_type_cnt,
            click_product_detail_cnt,
            click_compare_cnt,
            click_coupon_cnt,
            click_penalty_cnt,
            click_change_cnt,
            click_change_success_cnt,
            product_type_clicks,
            product_type_top_tags
            """;

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final SqlFileLoader sqlFileLoader;
    private final AthenaFeatureSyncProperties properties;

    public int deleteBySnapshotDate(String snapshotDate) {
        // 재실행 안전성을 위해 동일 snapshotDate 데이터를 먼저 제거한다.
        String sql = sqlFileLoader.load(DELETE_SQL_PATH);
        return namedParameterJdbcTemplate.update(sql, new MapSqlParameterSource("snapshotDate", snapshotDate));
    }

    public void importFromS3(String bucket, String key) {
        // 최종 테이블에 직접 CSV를 적재한다.
        // 컬럼 순서는 Athena 결과 CSV의 헤더와 동일해야 한다.
        String sql = """
                SELECT aws_s3.table_import_from_s3(
                    '%s',
                    '%s',
                    '(format csv, header true)',
                    aws_commons.create_s3_uri('%s', '%s', '%s')
                )
                """.formatted(
                TARGET_TABLE,
                TARGET_COLUMNS.replace("\r", "").replace("\n", " ").trim(),
                escapeLiteral(bucket),
                escapeLiteral(key),
                escapeLiteral(properties.getAwsRegion())
        );

        namedParameterJdbcTemplate.getJdbcTemplate().execute(sql);
    }

    public long countBySnapshotDate(String snapshotDate) {
        // 적재 결과가 최소 1건 이상인지 확인하는 검증용 조회다.
        String sql = sqlFileLoader.load(VERIFY_SQL_PATH);
        Long count = namedParameterJdbcTemplate.queryForObject(
                sql,
                new MapSqlParameterSource("snapshotDate", snapshotDate),
                Long.class
        );
        return count == null ? 0L : count;
    }

    private String escapeLiteral(String value) {
        // aws_commons.create_s3_uri 인자로 들어가는 문자열 literal escaping 용도다.
        return value.replace("'", "''");
    }
}
