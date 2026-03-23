package site.holliverse.worker.batch.jobs.athena.support;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

/**
 * Athena SDK Client bean 설정.
 *
 * Spring 컨테이너가 AthenaClient를 한 번 생성해 재사용하도록 등록한다.
 * 리전은 AthenaFeatureSyncProperties에서 읽는다.
 */
@Configuration
public class AwsAthenaConfig {

    @Bean
    public AthenaClient athenaClient(AthenaFeatureSyncProperties properties) {
        // Athena는 리전 정보가 필수다. 현재 기본값은 서울 리전(ap-northeast-2)이다.
        return AthenaClient.builder()
                .region(Region.of(properties.getAwsRegion()))
                .build();
    }
}
