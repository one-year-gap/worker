package site.holliverse.worker.batch.jobs.memberllmcontext;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import site.holliverse.worker.batch.jobs.memberllmcontext.tasklet.BuildMemberLlmContextTasklet;
import site.holliverse.worker.batch.jobs.memberllmcontext.tasklet.MemberLlmContextGateTasklet;
import site.holliverse.worker.batch.jobs.memberllmcontext.tasklet.VerifyMemberLlmContextTasklet;

/**
 * member_llm_context 적재 전용 배치 잡 구성.
 *
 * 흐름은 단순하게 세 단계로 나눈다.
 * 1. Gate: 기준일과 전월 yyyymm 계산, 필수 테이블 확인
 * 2. Upsert: 회원 컨텍스트를 한 번에 계산해서 upsert
 * 3. Verify: 적재 대상 수와 결과 수가 맞는지 최소 검증
 */
@Configuration
@RequiredArgsConstructor
public class MemberLlmContextJobConfig {

    public static final String JOB_NAME = "memberLlmContextJob";

    @Bean
    public Job memberLlmContextJob(
            JobRepository jobRepository,
            Step memberLlmContextGateStep,
            Step memberLlmContextUpsertStep,
            Step memberLlmContextVerifyStep
    ) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(memberLlmContextGateStep)
                .next(memberLlmContextUpsertStep)
                .next(memberLlmContextVerifyStep)
                .build();
    }

    /**
     * 배치 파라미터와 실행 환경을 정리하는 선행 step.
     */
    @Bean
    public Step memberLlmContextGateStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            MemberLlmContextGateTasklet tasklet
    ) {
        return new StepBuilder("Step00_Gate", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    /**
     * 실제 member_llm_context upsert SQL을 수행하는 핵심 step.
     */
    @Bean
    public Step memberLlmContextUpsertStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            BuildMemberLlmContextTasklet tasklet
    ) {
        return new StepBuilder("Step01_UpsertMemberLlmContext", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }

    /**
     * 적재 결과 건수와 필수 컬럼을 검증하는 마무리 step.
     */
    @Bean
    public Step memberLlmContextVerifyStep(
            JobRepository jobRepository,
            PlatformTransactionManager tx,
            VerifyMemberLlmContextTasklet tasklet
    ) {
        return new StepBuilder("Step02_Verify", jobRepository)
                .tasklet(tasklet, tx)
                .build();
    }
}