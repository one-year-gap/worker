package site.holliverse.worker.batch.jobs.athena.support;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.common.support.SqlFileLoader;

/**
 * Athena SQL 템플릿 로딩 및 snapshotDate 바인딩 담당 클래스.
 *
 * SQL 파일 경로와 템플릿 처리 책임을 Tasklet에서 분리해
 * Step 로직은 "언제 실행할지"에만 집중하고,
 * 이 클래스는 "어떤 SQL을 만들지"를 담당하도록 나눈다.
 */
@Component
@RequiredArgsConstructor
public class AthenaSqlTemplateFactory {

    // 현재 user_event_features_7d 적재용 Athena 집계 쿼리 위치.
    private static final String SQL_PATH = "sql/athena/user_event_features_7d.sql";

    private final SqlFileLoader sqlFileLoader;

    public String buildUserEventFeaturesQuery(String snapshotDate) {
        // SQL 템플릿은 리소스 파일로 관리하고
        // 실행 시점에는 snapshotDate만 주입해 최종 쿼리를 만든다.
        String template = sqlFileLoader.load(SQL_PATH);
        return template.formatted(snapshotDate);
    }
}
