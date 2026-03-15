package site.holliverse.worker.batch.jobs.athena.support;

import org.springframework.stereotype.Component;

/**
 * Athena OutputLocation 문자열을 bucket / key로 분리하는 도우미.
 *
 * Athena는 결과 파일 경로를 s3://bucket/key 형식 문자열 하나로 돌려준다.
 * 반면 PostgreSQL aws_s3.table_import_from_s3는 bucket, key, region을 따로 받으므로
 * import 직전에 이 파싱 단계가 필요하다.
 */
@Component
public class S3OutputLocationParser {

    public ParsedS3Path parse(String s3Uri) {
        // Athena 결과 경로는 항상 s3:// 로 시작해야 한다.
        if (s3Uri == null || !s3Uri.startsWith("s3://")) {
            throw new IllegalArgumentException("Invalid S3 URI: " + s3Uri);
        }

        String withoutScheme = s3Uri.substring("s3://".length());
        int firstSlash = withoutScheme.indexOf('/');

        if (firstSlash < 0) {
            return new ParsedS3Path(withoutScheme, "");
        }

        String bucket = withoutScheme.substring(0, firstSlash);
        String key = withoutScheme.substring(firstSlash + 1);

        return new ParsedS3Path(bucket, key);
    }

    /**
     * 파싱 결과를 담는 단순 record.
     *
     * 예:
     * - bucket: holliverse-log
     * - key: athena-results/feature-sync/uuid.csv
     */
    public record ParsedS3Path(String bucket, String key) {
    }
}
