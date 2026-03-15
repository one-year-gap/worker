package site.holliverse.worker.batch.jobs.athena.support;

/**
 * Athena feature sync Job이 ExecutionContext에 저장하는 key 모음.
 *
 * ExecutionContext는 Step 간에 값을 전달하는 공유 저장소다.
 * 이 Job에서는 Athena 실행 결과와 snapshotDate를 여러 Step이 순차적으로 재사용하므로
 * 문자열 상수를 한 곳에 모아 오타와 중복을 줄인다.
 */
public final class AthenaFeatureSyncContextKeys {

    private AthenaFeatureSyncContextKeys() {
    }

    // 배치 실행 기준 날짜. Gate Step에서 확정한 뒤 이후 모든 Step이 사용한다.
    public static final String SNAPSHOT_DATE = "snapshotDate";

    // Athena StartQueryExecution 호출 후 즉시 반환되는 실행 ID.
    public static final String QUERY_EXECUTION_ID = "queryExecutionId";

    // Athena가 성공적으로 생성한 결과 CSV의 전체 S3 URI.
    public static final String OUTPUT_LOCATION = "outputLocation";

    // OUTPUT_LOCATION에서 추출한 S3 bucket 이름.
    public static final String OUTPUT_BUCKET = "outputBucket";

    // OUTPUT_LOCATION에서 추출한 S3 object key.
    public static final String OUTPUT_KEY = "outputKey";

    // 최종 검증 단계에서 계산한 적재 건수.
    public static final String IMPORTED_ROW_COUNT = "importedRowCount";
}
