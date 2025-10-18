package com.ryuqq.orchestrator.adapter.runner;

import com.ryuqq.orchestrator.application.runtime.Runtime;
import com.ryuqq.orchestrator.core.contract.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ryuqq.orchestrator.core.executor.Executor;
import com.ryuqq.orchestrator.core.model.OpId;
import com.ryuqq.orchestrator.core.outcome.Fail;
import com.ryuqq.orchestrator.core.outcome.Ok;
import com.ryuqq.orchestrator.core.outcome.Outcome;
import com.ryuqq.orchestrator.core.outcome.Retry;
import com.ryuqq.orchestrator.core.spi.Bus;
import com.ryuqq.orchestrator.core.spi.Store;
import com.ryuqq.orchestrator.core.statemachine.OperationState;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Queue Worker Runner 구현체.
 *
 * <p>메시지 큐에서 작업을 가져와 실행하고, Outcome에 따라 처리합니다.</p>
 *
 * <p><strong>책임:</strong></p>
 * <ul>
 *   <li>메시지 큐에서 Envelope batch dequeue</li>
 *   <li>Executor를 통한 작업 실행</li>
 *   <li>Outcome에 따른 분기 처리 (Ok, Retry, Fail)</li>
 *   <li>Write-ahead 로깅 및 상태 finalization</li>
 *   <li>메시지 ACK/NACK 처리</li>
 * </ul>
 *
 * <p><strong>처리 흐름:</strong></p>
 * <pre>
 * pump() 호출
 *   ↓
 * dequeue(batchSize) → [Envelope1, Envelope2, ...]
 *   ↓
 * For each Envelope:
 *   1. executor.execute(envelope) → 비동기 시작
 *   2. pollForCompletion(opId) → 상태 폴링
 *   3. getOutcome(opId) → Outcome 조회
 *   4. handleOutcome(outcome):
 *      - Ok → writeAhead + finalize(COMPLETED)
 *      - Retry → RetryBudget 확인 + backoff + re-publish or finalize(FAILED)
 *      - Fail → finalize(FAILED) + DLQ
 *   5. bus.ack(envelope) → 처리 완료
 * </pre>
 *
 * <p><strong>동시성 제어:</strong></p>
 * <ul>
 *   <li>dequeue는 동기화되어 중복 dequeue 방지</li>
 *   <li>개별 Envelope 처리는 동시 실행 가능 (concurrency 제한)</li>
 *   <li>동일 OpId 중복 처리 방지 (Bus의 visibility timeout 활용)</li>
 * </ul>
 *
 * @author Orchestrator Team
 * @since 1.0.0
 */
public final class QueueWorkerRunner implements Runtime {

    private static final Logger log = LoggerFactory.getLogger(QueueWorkerRunner.class);
    private static final long DEFAULT_POLLING_INTERVAL_MS = 10;

    private final Bus bus;
    private final Store store;
    private final Executor executor;
    private final QueueWorkerConfig config;
    private final BackoffCalculator backoffCalculator;
    private final ExecutorService workerExecutor;

    /**
     * 생성자 (기본 BackoffCalculator 사용).
     *
     * @param bus 메시지 버스
     * @param store 저장소
     * @param executor 작업 실행자
     * @param config 설정
     * @throws IllegalArgumentException 의존성이 null인 경우
     */
    public QueueWorkerRunner(Bus bus, Store store, Executor executor, QueueWorkerConfig config) {
        this(bus, store, executor, config, new BackoffCalculator());
    }

    /**
     * 생성자 (커스텀 BackoffCalculator 주입).
     *
     * @param bus 메시지 버스
     * @param store 저장소
     * @param executor 작업 실행자
     * @param config 설정
     * @param backoffCalculator 백오프 계산기
     * @throws IllegalArgumentException 의존성이 null인 경우
     */
    public QueueWorkerRunner(Bus bus, Store store, Executor executor, QueueWorkerConfig config, BackoffCalculator backoffCalculator) {
        if (bus == null) {
            throw new IllegalArgumentException("bus cannot be null");
        }
        if (store == null) {
            throw new IllegalArgumentException("store cannot be null");
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        if (backoffCalculator == null) {
            throw new IllegalArgumentException("backoffCalculator cannot be null");
        }

        this.bus = bus;
        this.store = store;
        this.executor = executor;
        this.config = config;
        this.backoffCalculator = backoffCalculator;

        // Named ThreadFactory for better observability
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "queue-worker-" + threadNumber.getAndIncrement());
            }
        };

        this.workerExecutor = Executors.newFixedThreadPool(config.concurrency(), threadFactory);
    }

    @Override
    public void pump() {
        // 1. 메시지 큐에서 배치 dequeue
        List<Envelope> envelopes = bus.dequeue(config.batchSize());

        // 2. 각 Envelope을 병렬 처리 (ExecutorService에 제출)
        for (Envelope envelope : envelopes) {
            workerExecutor.submit(() -> processEnvelope(envelope));
        }
    }

    /**
     * Runner 종료 (리소스 정리).
     *
     * <p>ExecutorService를 graceful shutdown하여 진행 중인 작업이
     * 완료되도록 대기합니다.</p>
     *
     * @throws InterruptedException shutdown 대기 중 인터럽트 발생 시
     */
    public void shutdown() throws InterruptedException {
        workerExecutor.shutdown();
        if (!workerExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
            workerExecutor.shutdownNow();
        }
    }

    /**
     * Envelope 처리 (실행 → Outcome 대기 → 처리 → ACK).
     *
     * <p>예외 발생 시 NACK 처리하여 재시도 가능하도록 합니다.</p>
     *
     * @param envelope 처리할 Envelope
     */
    private void processEnvelope(Envelope envelope) {
        OpId opId = envelope.opId();

        try {
            // 1. 실행 시작 (비동기)
            executor.execute(envelope);

            // 2. 완료 대기 (polling)
            pollForCompletion(opId);

            // 3. Outcome 조회
            Outcome outcome = executor.getOutcome(opId);

            // 4. Outcome 처리
            handleOutcome(opId, outcome, envelope);

            // 5. 메시지 ACK (성공)
            bus.ack(envelope);

        } catch (Exception e) {
            // 예외 발생 시 NACK (재처리 가능하도록)
            bus.nack(envelope);
            throw new RuntimeException("Failed to process envelope: " + opId, e);
        }
    }

    /**
     * Outcome 분기 처리 (Ok, Retry, Fail).
     *
     * @param opId Operation ID
     * @param outcome Outcome (Ok, Retry, Fail)
     * @param envelope 원본 Envelope (Retry 재게시 시 사용)
     */
    private void handleOutcome(OpId opId, Outcome outcome, Envelope envelope) {
        switch (outcome) {
            case Ok ok -> handleOk(opId, ok);
            case Retry retry -> handleRetry(opId, retry, envelope);
            case Fail fail -> handleFail(opId, fail, envelope);
        }
    }

    /**
     * Ok 처리: writeAhead → finalize(COMPLETED).
     *
     * <p>finalize 실패 시에도 계속 진행하며, Finalizer가 나중에 복구합니다.</p>
     *
     * @param opId Operation ID
     * @param ok Ok Outcome
     */
    private void handleOk(OpId opId, Ok ok) {
        // 1. writeAhead (영속성 보장)
        store.writeAhead(opId, ok);

        // 2. finalize (실패해도 Finalizer가 복구)
        try {
            store.finalize(opId, OperationState.COMPLETED);
        } catch (Exception e) {
            // 로그만 남기고 계속 진행 (Finalizer가 처리)
            log.warn("finalize failed for {}, will be handled by Finalizer", opId, e);
        }
    }

    /**
     * Retry 처리: RetryBudget 확인 → 재게시 or finalize(FAILED).
     *
     * <p>RetryBudget이 남아있으면 exponential backoff 적용 후 재게시하고,
     * 소진되었으면 finalize(FAILED)로 종료합니다.</p>
     *
     * @param opId Operation ID
     * @param retry Retry Outcome
     * @param envelope 원본 Envelope (재게시용)
     */
    private void handleRetry(OpId opId, Retry retry, Envelope envelope) {
        int attemptCount = retry.attemptCount();

        if (attemptCount < config.maxRetries()) {
            // 재시도 가능
            long delay = backoffCalculator.calculate(attemptCount);
            bus.publish(envelope, delay);
            log.info("Retry scheduled for {} after {}ms (attempt {})", opId, delay, attemptCount);
        } else {
            // RetryBudget 소진
            store.finalize(opId, OperationState.FAILED);
            log.warn("RetryBudget exhausted for {}, marked as FAILED", opId);
        }
    }

    /**
     * Fail 처리: finalize(FAILED) + DLQ.
     *
     * <p>영구 실패로 판단하여 즉시 종료 처리하고, DLQ로 전송합니다.</p>
     *
     * @param opId Operation ID
     * @param fail Fail Outcome
     * @param envelope 원본 Envelope (DLQ 전송용)
     */
    private void handleFail(OpId opId, Fail fail, Envelope envelope) {
        // 1. 즉시 실패 처리
        store.finalize(opId, OperationState.FAILED);
        log.error("Operation {} failed: {} - {}", opId, fail.errorCode(), fail.message());

        // 2. DLQ 전송 (선택적)
        if (config.dlqEnabled()) {
            bus.publishToDLQ(envelope, fail);
        }
    }

    /**
     * 작업 완료 대기 (소프트 폴링).
     *
     * <p>maxProcessingTimeMs 동안 주기적으로 상태를 체크하여
     * terminal 상태가 될 때까지 대기합니다.</p>
     *
     * <p>타임아웃 발생 시 RuntimeException을 던집니다.</p>
     *
     * @param opId Operation ID
     * @throws RuntimeException 타임아웃 발생 시
     */
    private void pollForCompletion(OpId opId) {
        long startTimeNanos = System.nanoTime();
        long timeoutNanos = config.maxProcessingTimeMs() * 1_000_000L;

        while (System.nanoTime() - startTimeNanos < timeoutNanos) {
            OperationState state = executor.getState(opId);

            if (state.isTerminal()) {
                return; // 완료
            }

            // 아직 진행 중: sleep 후 계속 폴링
            sleep(DEFAULT_POLLING_INTERVAL_MS);
        }

        // 타임아웃
        throw new RuntimeException("Operation " + opId + " timed out after " +
            config.maxProcessingTimeMs() + "ms");
    }

    /**
     * Sleep (폴링 간격 대기).
     *
     * <p>InterruptedException 발생 시 현재 스레드의 인터럽트 플래그를 복원하고
     * RuntimeException으로 래핑하여 던집니다.</p>
     *
     * @param millis 대기 시간 (밀리초)
     * @throws RuntimeException sleep 중 인터럽트 발생 시
     */
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Polling interrupted", e);
        }
    }
}
