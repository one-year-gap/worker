package site.holliverse.worker.batch.consultation.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Component
@RequiredArgsConstructor
public class ConsultationAnalysisSql {
    private final ResourceLoader resourceLoader;

    //선점 query
    public String claimChunk() {
        return read("sql/consultation/claim_chunk.sql");
    }

    //claim_token으로 상담 객체 조회
    public String loadClaimedCase(){
        return read("sql/consultation/load_claimed_case.sql");
    }

    /**
     *
     * @return
     */
    public String upsertDispatchOutbox() {
        return read("sql/consultation/upsert_dispatch_outbox.sql");
    }

    public String markDispatchSent() {
        return read("sql/consultation/mark_dispatch_sent.sql");
    }

    public String markDispatchRetry() {
        return read("sql/consultation/mark_dispatch_retry.sql");
    }


    private String read(String path) {
        try (InputStream in = resourceLoader.getResource("classpath:" + path).getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("SQL load failed: " + path, e);
        }
    }
}
