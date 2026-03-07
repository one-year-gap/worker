package site.holliverse.worker.batch.consultation.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import site.holliverse.worker.batch.consultation.model.DispatchTarget;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class ConsultationAnalysisClaimRepository {
    private final JdbcClient jdbcClient;
    private final ConsultationAnalysisSql sql;


    /**
     * row 단위 선점 진행
     */
    public List<Long> claimChunk(long analyzerVersion,long jobInstanceId,int chunkSize,int leaseSec,long claimToken){
        return jdbcClient.sql(sql.claimChunk())
                .param("ver",analyzerVersion)
                .param("job",jobInstanceId)
                .param("chunk",chunkSize)
                .param("lease_sec",leaseSec)
                .param("claim_token",claimToken)
                .query((rs, rowNum) -> rs.getLong("analysis_id"))
                .list();
    }

    /**
     * claimToken으로 조회
     */
    public List<DispatchTarget> findByClaimToken(long claimToken){
        if (claimToken<0L) return List.of();

        return jdbcClient.sql(sql.loadClaimedCase())
                .param("claim_token", claimToken)
                .query((rs,rowNum)->new DispatchTarget(
                        rs.getLong("analysis_id"),
                        rs.getLong("case_id"),
                        rs.getLong("analyzer_version")
                ))
                .list();
    }
}
