package site.holliverse.worker.batch.common.support;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Component
public class SqlFileLoader {

    // classpath SQL 파일을 UTF-8 문자열로 읽어 Tasklet에서 재사용한다.
    public String load(String classpathLocation) {
        ClassPathResource resource = new ClassPathResource(classpathLocation);
        try {
            return StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("SQL 파일을 읽는 중 오류가 발생했습니다: " + classpathLocation, e);
        }
    }
}
