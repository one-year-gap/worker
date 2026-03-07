package site.holliverse.worker.batch.consultation.step;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import site.holliverse.worker.batch.consultation.model.AnalysisRequestMessage;
import site.holliverse.worker.batch.consultation.model.DispatchTarget;
import site.holliverse.worker.batch.consultation.repository.ConsultationAnalysisClaimRepository;
import site.holliverse.worker.batch.consultation.repository.ConsultationDispatchOutboxRepository;
import site.holliverse.worker.global.util.RandomIdCreator;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class DispatchTasklet implements Tasklet {
    private final RandomIdCreator randomIdCreator;
    private final ConsultationAnalysisClaimRepository repository;
    private final ConsultationDispatchOutboxRepository outboxRepository;
    private final KafkaTemplate<String, AnalysisRequestMessage> analysisKafkaTemplate;

    @Value("${worker.kafka.topic.analysis-request:analysis.request.v1}")
    private String analysisRequestTopic;

    @Value("${worker.kafka.outbox.max-attempts:3}")
    private int maxOutboxAttempts;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ExecutionContext ctx = contribution.getStepExecution().getJobExecution().getExecutionContext();

        //token 추출
        long claimToken = ctx.getLong(ClaimStepTasklet.CTX_CLAIM_TOKEN, -1L);
        //선점 개수 추출
        int claimedCount = ctx.getInt(ClaimStepTasklet.CTX_CLAIMED_COUNT,0);
        long jobInstanceId = ctx.getLong(
                ClaimStepTasklet.CTX_JOB_INSTANCE_ID,
                contribution.getStepExecution().getJobExecution().getJobId()
        );
        String chunkId = String.valueOf(claimToken);

        if (claimToken < 0) throw new IllegalStateException("missing claim token");
        if (claimedCount==0) return RepeatStatus.FINISHED;

        //target 상담 데이터 조회
        List<DispatchTarget> targets = repository.findByClaimToken(claimToken);
        if (targets.isEmpty()) {
            return RepeatStatus.FINISHED;
        }

        int published = 0;
        int skipped = 0;
        int retried = 0;

        //타겟 상담 데이터 트랜잭션 시작
        for (DispatchTarget target : targets){
            //분석 요청 Id 생성
            String requestId = randomIdCreator.createDispatchRequestId(target.caseId(), target.analyzerVersion());
            //발송 전 초기 아웃박스 객체 UPSERT
            String outboxStatus = outboxRepository.upsertReadyAndGetStatus(
                    requestId,
                    jobInstanceId,
                    chunkId,
                    String.valueOf(target.analyzerVersion())
            );

            //발송 상태 - 전송 중 || 수신확인 상태 => skip
            if ("SENT".equals(outboxStatus) || "ACKED".equals(outboxStatus)) {
                skipped++;
                continue;
            }
            //발송 상태 - 발송 영구 싪패 => skip
            if ("DEAD".equals(outboxStatus)) {
                skipped++;
                continue;
            }

            //분석 요청 메세지 발급
            AnalysisRequestMessage message = new AnalysisRequestMessage(
                    requestId,
                    target.caseId(),
                    target.analyzerVersion()
            );
            try {
                //kafka 발송
                analysisKafkaTemplate.send(analysisRequestTopic, requestId, message).join();
                //dispatch_outbox_status = 'SENT' 변경
                outboxRepository.markSent(requestId);
                published++;
            } catch (Exception ex) {
                //발송 실패 시 재시도 정책 시행
                String nextStatus = outboxRepository.markRetry(
                        requestId,
                        truncateError(ex),
                        maxOutboxAttempts
                );
                retried++;
                log.warn(
                        "Dispatch failed. requestId={}, caseId={}, analyzerVersion={}, nextStatus={}",
                        requestId,
                        target.caseId(),
                        target.analyzerVersion(),
                        nextStatus
                );
            }
        }

        return RepeatStatus.FINISHED;
    }


    private String truncateError(Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isBlank()) {
            message = ex.getClass().getSimpleName();
        }
        if (message.length() > 1000) {
            return message.substring(0, 1000);
        }
        return message;
    }
}
