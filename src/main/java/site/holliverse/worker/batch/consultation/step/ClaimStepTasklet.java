package site.holliverse.worker.batch.consultation.step;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.consultation.repository.ConsultationAnalysisClaimRepository;
import site.holliverse.worker.global.util.RandomIdCreator;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class ClaimStepTasklet implements Tasklet {
    public static final String EXIT_NO_CLAIMED_CASES = "NO_CLAIMED_CASES";
    public static final String CTX_CLAIM_TOKEN = "consultation.claim.token";
    public static final String CTX_CLAIMED_COUNT = "consultation.claim.claimedCount";
    public static final String CTX_JOB_INSTANCE_ID = "consultation.claim.jobInstanceId";

    private final RandomIdCreator randomIdCreator;
    private final ConsultationAnalysisClaimRepository repository;

    @Value("${worker.job.consultation-analysis.analyzer-version}")
    private long analyzerVersion;

    @Value("${worker.job.consultation-analysis.claim-chunk-size}")
    private int chunkSize;

    @Value("${worker.job.consultation-analysis.claim-lease-sec}")
    private int claimLeaseSec;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        //JobInstanceId 추출
        JobExecution jobExecution = contribution.getStepExecution().getJobExecution();
        long jobInstanceId = jobExecution.getJobId();
        ExecutionContext executionContext = jobExecution.getExecutionContext();

        //claimToken 생성
        long token = randomIdCreator.createTsid();

        //target 상담 데이터 조회
        List<Long> claimedAnalysisIds = repository.claimChunk(
                analyzerVersion,
                jobInstanceId,
                chunkSize,
                claimLeaseSec,
                token
        );

        //다음 step 변수 넘기기
        executionContext.putLong(CTX_CLAIM_TOKEN, token);
        executionContext.putInt(CTX_CLAIMED_COUNT, claimedAnalysisIds.size());
        executionContext.putLong(CTX_JOB_INSTANCE_ID, jobInstanceId);

        //target 상담 데이터가 빈 값이면 Job 종료
        if (claimedAnalysisIds.isEmpty()) {
            contribution.setExitStatus(new ExitStatus(EXIT_NO_CLAIMED_CASES));
            return RepeatStatus.FINISHED;
        }

        return RepeatStatus.FINISHED;
    }

}
