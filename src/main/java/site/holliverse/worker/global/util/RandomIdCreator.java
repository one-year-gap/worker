package site.holliverse.worker.global.util;

import com.github.f4b6a3.tsid.TsidFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.HexFormat;
import java.util.UUID;

@Component
public class RandomIdCreator {
    private static final int NODE_BITS = 8; // 0~255
    private static final int MAX_NODE = (1 << NODE_BITS) - 1;
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private final TsidFactory tsidFactory;

    public RandomIdCreator(@Value("${spring.time.zone}") String timeZone,
                           @Value("${worker.id.tsid-node:0}") Integer tsidNode) {
        int normalizedNode = normalizeNode(tsidNode);
        this.tsidFactory = TsidFactory.builder()
                .withNodeBits(NODE_BITS)
                .withNode(normalizedNode)
                .withClock(Clock.system(ZoneId.of(timeZone)))
                .build();
    }

    /**
     * chunk 고유 Id 생성
     *
     * @return String: chunk Id
     */
    public String createChunkId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 발송 전용 Id 생성
     *
     * @param caseId           상담 Id
     * @param analyzerVersion  분석기 버전
     * @return String:         dispatchRequestId
     */
    public String createDispatchRequestId(long caseId, long analyzerVersion) {
        return sha256Hex(caseId + ":" + analyzerVersion);
    }

    public String createDispatchRequestId(String caseId, String analyzerVersion) {
        if (caseId == null || caseId.isBlank()) {
            throw new IllegalArgumentException("caseId must not be blank");
        }
        if (analyzerVersion == null || analyzerVersion.isBlank()) {
            throw new IllegalArgumentException("analyzerVersion must not be blank");
        }
        return sha256Hex(caseId + ":" + analyzerVersion);
    }

    /**
     * TSID 생성
     *
     * @return TSID
     */
    public long createTsid() {
        return tsidFactory.create().toLong();
    }


    private String sha256Hex(String source) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(source.getBytes(StandardCharsets.UTF_8));
            return HEX_FORMAT.formatHex(hashed);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", e);
        }
    }

    private int normalizeNode(Integer tsidNode) {
        int node = tsidNode == null ? 0 : tsidNode;
        if (node < 0 || node > MAX_NODE) {
            throw new IllegalArgumentException("worker.id.tsid-node must be between 0 and " + MAX_NODE);
        }
        return node;
    }

}
