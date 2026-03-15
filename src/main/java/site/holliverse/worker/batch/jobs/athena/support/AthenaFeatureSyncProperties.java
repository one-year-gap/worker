package site.holliverse.worker.batch.jobs.athena.support;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Athena feature sync Job 전용 설정 묶음.
 *
 * application.worker.yml 의
 * worker.job.athena-feature-sync.* 값을 이 객체로 자동 매핑한다.
 *
 * 예:
 * - aws-region -> awsRegion
 * - output-location -> outputLocation
 * - max-poll-count -> maxPollCount
 */
@Configuration
@ConfigurationProperties(prefix = "worker.job.athena-feature-sync")
public class AthenaFeatureSyncProperties {

    // Athena와 S3가 동작하는 AWS 리전. 현재 기본값은 서울 리전이다.
    private String awsRegion = "ap-northeast-2";

    // Athena 쿼리를 실행할 database 이름.
    private String database = "holliverse_analytics";

    // Athena Workgroup 이름. 비워두면 request에 workgroup을 넣지 않는다.
    private String workgroup;

    // Athena 결과 CSV 저장 prefix. raw 로그 LOCATION과는 다른 값이다.
    private String outputLocation;

    // Athena 완료 polling 간격(ms).
    private long pollIntervalMillis = 5000L;

    // Athena polling 최대 횟수. 사실상 timeout 역할을 한다.
    private int maxPollCount = 120;

    public String getAwsRegion() {
        return awsRegion;
    }

    public void setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getWorkgroup() {
        return workgroup;
    }

    public void setWorkgroup(String workgroup) {
        this.workgroup = workgroup;
    }

    public String getOutputLocation() {
        return outputLocation;
    }

    public void setOutputLocation(String outputLocation) {
        this.outputLocation = outputLocation;
    }

    public long getPollIntervalMillis() {
        return pollIntervalMillis;
    }

    public void setPollIntervalMillis(long pollIntervalMillis) {
        this.pollIntervalMillis = pollIntervalMillis;
    }

    public int getMaxPollCount() {
        return maxPollCount;
    }

    public void setMaxPollCount(int maxPollCount) {
        this.maxPollCount = maxPollCount;
    }
}
