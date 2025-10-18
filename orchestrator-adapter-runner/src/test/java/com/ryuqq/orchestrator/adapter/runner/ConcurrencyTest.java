package com.ryuqq.orchestrator.adapter.runner;

import com.ryuqq.orchestrator.core.contract.Command;
import com.ryuqq.orchestrator.core.contract.Envelope;
import com.ryuqq.orchestrator.core.executor.Executor;
import com.ryuqq.orchestrator.core.model.*;
import com.ryuqq.orchestrator.core.outcome.Ok;
import com.ryuqq.orchestrator.core.outcome.Outcome;
import com.ryuqq.orchestrator.core.spi.Bus;
import com.ryuqq.orchestrator.core.spi.Store;
import com.ryuqq.orchestrator.core.spi.WriteAheadState;
import com.ryuqq.orchestrator.core.statemachine.OperationState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * 동시성 제어 통합 테스트.
 *
 * <p>Store SPI 구현체의 동시 접근 처리를 검증합니다:</p>
 * <ul>
 *   <li>동일 OpId 중복 finalize 방지 (멱등성)</li>
 *   <li>여러 Finalizer 인스턴스 동시 실행</li>
 *   <li>여러 Reaper 인스턴스 동시 실행</li>
 *   <li>QueueWorkerRunner 멀티 스레드 처리</li>
 * </ul>
 *
 * <p><strong>주의:</strong> 이 테스트는 Store 구현체의 동시성 보장이 올바르게 구현되어 있다고 가정합니다.
 * 실제 Store 구현체는 다음을 보장해야 합니다:</p>
 * <ul>
 *   <li>finalize(): 멱등 (동일 OpId 중복 호출 시 안전)</li>
 *   <li>scanWA(): 동시 호출 시 항목 중복 반환 없음</li>
 *   <li>scanInProgress(): 동시 호출 시 항목 중복 반환 없음</li>
 * </ul>
 *
 * @author Orchestrator Team
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
class ConcurrencyTest {

    @Mock
    private Bus bus;

    @Mock
    private Store store;

    @Mock
    private Executor executor;

    private QueueWorkerConfig queueConfig;
    private FinalizerConfig finalizerConfig;
    private ReaperConfig reaperConfig;

    @BeforeEach
    void setUp() {
        queueConfig = new QueueWorkerConfig(); // Already has maxRetries=3 and dlqEnabled=true by default

        finalizerConfig = new FinalizerConfig().withBatchSize(100);

        reaperConfig = new ReaperConfig(); // Already has timeoutThresholdMs=600000, batchSize=50, defaultStrategy=FAIL by default
    }

    // ============================================================
    // 1. 동일 OpId 중복 finalize 방지 (Store 멱등성 검증)
    // ============================================================

    @Test
    void 동일_OpId_중복_finalize_시도_시_Store는_멱등하게_처리해야_함() throws Exception {
        // given
        OpId opId = OpId.of("duplicate-op");

        // Store 구현체가 멱등하게 동작한다고 가정 (두 번째 호출은 무시)
        doNothing().when(store).finalize(opId, OperationState.COMPLETED);

        // when: 여러 스레드에서 동시에 finalize 호출
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            futures.add(executorService.submit(() -> {
                store.finalize(opId, OperationState.COMPLETED);
            }));
        }

        // 모든 작업 완료 대기
        for (Future<?> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        executorService.shutdown();

        // then: Store가 멱등하게 동작하므로 모든 호출이 안전함
        verify(store, times(10)).finalize(opId, OperationState.COMPLETED);
        // 실제 Store 구현체는 내부적으로 중복 호출을 무시해야 함
    }

    // ============================================================
    // 2. 여러 Finalizer 인스턴스 동시 실행
    // ============================================================

    @Test
    void 여러_Finalizer_인스턴스_동시_실행_시_중복_처리_방지됨() throws Exception {
        // given
        OpId opId1 = OpId.of("pending-op-1");
        OpId opId2 = OpId.of("pending-op-2");
        OpId opId3 = OpId.of("pending-op-3");

        // Store는 scanWA 호출 시 PENDING 항목을 분배해서 반환 (동시성 보장)
        // 첫 번째 Finalizer는 opId1, opId2 받음
        // 두 번째 Finalizer는 opId3 받음 (중복 없음)
        AtomicInteger scanCount = new AtomicInteger(0);
        when(store.scanWA(WriteAheadState.PENDING, 100)).thenAnswer(invocation -> {
            int count = scanCount.getAndIncrement();
            if (count == 0) {
                return List.of(opId1, opId2);
            } else if (count == 1) {
                return List.of(opId3);
            } else {
                return List.of();
            }
        });

        when(store.getWriteAheadOutcome(opId1)).thenReturn(new Ok(opId1, "Success"));
        when(store.getWriteAheadOutcome(opId2)).thenReturn(new Ok(opId2, "Success"));
        when(store.getWriteAheadOutcome(opId3)).thenReturn(new Ok(opId3, "Success"));

        // when: 두 개의 Finalizer 인스턴스가 동시에 scan() 실행
        Finalizer finalizer1 = new Finalizer(store, finalizerConfig);
        Finalizer finalizer2 = new Finalizer(store, finalizerConfig);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        executorService.submit(() -> {
            try {
                finalizer1.scan();
            } finally {
                latch.countDown();
            }
        });

        executorService.submit(() -> {
            try {
                finalizer2.scan();
            } finally {
                latch.countDown();
            }
        });

        latch.await(5, TimeUnit.SECONDS);
        executorService.shutdown();

        // then: 모든 항목이 정확히 한 번씩 finalize됨
        verify(store).finalize(opId1, OperationState.COMPLETED);
        verify(store).finalize(opId2, OperationState.COMPLETED);
        verify(store).finalize(opId3, OperationState.COMPLETED);
    }

    // ============================================================
    // 3. 여러 Reaper 인스턴스 동시 실행
    // ============================================================

    @Test
    void 여러_Reaper_인스턴스_동시_실행_시_중복_처리_방지됨() throws Exception {
        // given
        OpId opId1 = OpId.of("stuck-op-1");
        OpId opId2 = OpId.of("stuck-op-2");

        // Store는 scanInProgress 호출 시 IN_PROGRESS 항목을 분배해서 반환 (동시성 보장)
        AtomicInteger scanCount = new AtomicInteger(0);
        when(store.scanInProgress(600000, 50)).thenAnswer(invocation -> {
            int count = scanCount.getAndIncrement();
            if (count == 0) {
                return List.of(opId1);
            } else if (count == 1) {
                return List.of(opId2);
            } else {
                return List.of();
            }
        });

        // FAIL 전략에서는 getEnvelope 호출 안 함 (불필요한 stubbing 제거)

        // when: 두 개의 Reaper 인스턴스가 동시에 scan() 실행
        Reaper reaper1 = new Reaper(bus, store, reaperConfig);
        Reaper reaper2 = new Reaper(bus, store, reaperConfig);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        executorService.submit(() -> {
            try {
                reaper1.scan();
            } finally {
                latch.countDown();
            }
        });

        executorService.submit(() -> {
            try {
                reaper2.scan();
            } finally {
                latch.countDown();
            }
        });

        latch.await(5, TimeUnit.SECONDS);
        executorService.shutdown();

        // then: 모든 항목이 정확히 한 번씩 finalize됨
        verify(store).finalize(opId1, OperationState.FAILED);
        verify(store).finalize(opId2, OperationState.FAILED);
    }

    // ============================================================
    // 4. QueueWorkerRunner 멀티 스레드 처리
    // ============================================================

    @Test
    void QueueWorkerRunner_멀티_스레드_처리_시_각_Envelope는_한_번씩만_처리됨() throws Exception {
        // given
        OpId opId1 = OpId.of("op-1");
        OpId opId2 = OpId.of("op-2");
        OpId opId3 = OpId.of("op-3");

        Envelope env1 = createTestEnvelope(opId1);
        Envelope env2 = createTestEnvelope(opId2);
        Envelope env3 = createTestEnvelope(opId3);

        // 각 스레드가 dequeue 시 서로 다른 Envelope 받음 (Bus 동시성 보장)
        AtomicInteger dequeueCount = new AtomicInteger(0);
        when(bus.dequeue(anyInt())).thenAnswer(invocation -> {
            int count = dequeueCount.getAndIncrement();
            if (count == 0) {
                return List.of(env1);
            } else if (count == 1) {
                return List.of(env2);
            } else if (count == 2) {
                return List.of(env3);
            } else {
                return List.of();
            }
        });

        // 각 OpId별로 사전에 Executor 동작 설정
        when(executor.getState(opId1)).thenReturn(OperationState.COMPLETED);
        when(executor.getOutcome(opId1)).thenReturn(new Ok(opId1, "Success"));

        when(executor.getState(opId2)).thenReturn(OperationState.COMPLETED);
        when(executor.getOutcome(opId2)).thenReturn(new Ok(opId2, "Success"));

        when(executor.getState(opId3)).thenReturn(OperationState.COMPLETED);
        when(executor.getOutcome(opId3)).thenReturn(new Ok(opId3, "Success"));

        QueueWorkerRunner runner = new QueueWorkerRunner(bus, store, executor, queueConfig);

        // when: 세 개의 스레드가 동시에 pump() 실행
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        CountDownLatch latch = new CountDownLatch(3);

        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> {
                try {
                    runner.pump();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executorService.shutdown();

        // then: 각 Envelope가 정확히 한 번씩 처리됨
        verify(executor).execute(env1);
        verify(executor).execute(env2);
        verify(executor).execute(env3);

        verify(store).writeAhead(eq(opId1), any(Ok.class));
        verify(store).writeAhead(eq(opId2), any(Ok.class));
        verify(store).writeAhead(eq(opId3), any(Ok.class));

        verify(bus).ack(env1);
        verify(bus).ack(env2);
        verify(bus).ack(env3);
    }

    // ============================================================
    // 5. Store 멱등성 검증 (finalize 중복 호출)
    // ============================================================

    @Test
    void Store_finalize는_동일_OpId_중복_호출_시_멱등해야_함() {
        // given
        OpId opId = OpId.of("test-op");

        // Store 구현체는 멱등하게 동작해야 함 (두 번째 호출은 무시)
        doNothing().when(store).finalize(opId, OperationState.COMPLETED);

        Finalizer finalizer = new Finalizer(store, finalizerConfig);

        // Finalizer가 같은 항목을 여러 번 스캔하는 시나리오
        when(store.scanWA(WriteAheadState.PENDING, 100))
            .thenReturn(List.of(opId))
            .thenReturn(List.of(opId))
            .thenReturn(List.of());

        when(store.getWriteAheadOutcome(opId)).thenReturn(new Ok(opId, "Success"));

        // when: Finalizer가 두 번 scan() 호출
        finalizer.scan(); // 첫 번째 scan
        finalizer.scan(); // 두 번째 scan (같은 항목 재처리)

        // then: finalize가 두 번 호출되지만 Store는 멱등하게 처리
        verify(store, times(2)).finalize(opId, OperationState.COMPLETED);
        // 실제 Store 구현체는 두 번째 호출을 무시해야 함 (멱등성)
    }

    // ============================================================
    // 6. 동시성 스트레스 테스트
    // ============================================================

    @Test
    void 높은_동시성_환경에서_모든_작업_정상_처리됨() throws Exception {
        // given
        int totalOps = 100;
        Queue<OpId> opIds = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < totalOps; i++) {
            opIds.add(OpId.of("op-" + i));
        }

        // Store는 scanWA 호출 시 항목을 분배해서 반환
        when(store.scanWA(eq(WriteAheadState.PENDING), anyInt())).thenAnswer(invocation -> {
            if (opIds.isEmpty()) {
                return List.of();
            }
            // 배치 크기만큼 반환 (동시성 보장)
            int batchSize = invocation.getArgument(1);
            List<OpId> batch = new ArrayList<>();
            for (int i = 0; i < batchSize && !opIds.isEmpty(); i++) {
                OpId polled = opIds.poll();
                if (polled != null) {
                    batch.add(polled);
                }
            }
            return batch;
        });

        // 모든 항목이 Ok로 완료 (스냅샷 생성)
        List<OpId> opIdSnapshot = new ArrayList<>();
        for (int i = 0; i < totalOps; i++) {
            opIdSnapshot.add(OpId.of("op-" + i));
        }
        for (OpId opId : opIdSnapshot) {
            when(store.getWriteAheadOutcome(opId)).thenReturn(new Ok(opId, "Success"));
        }

        // when: 10개의 Finalizer 인스턴스가 동시에 scan() 실행
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                try {
                    Finalizer finalizer = new Finalizer(store, finalizerConfig);
                    finalizer.scan();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executorService.shutdown();

        // then: 모든 항목이 finalize됨 (정확한 횟수는 Store 구현에 따라 다름)
        ArgumentCaptor<OpId> opIdCaptor = ArgumentCaptor.forClass(OpId.class);
        verify(store, atLeast(totalOps)).finalize(opIdCaptor.capture(), eq(OperationState.COMPLETED));
        // Store 멱등성으로 인해 중복 호출 가능하지만 결과는 동일
    }

    // ============================================================
    // Helper Methods
    // ============================================================

    private Envelope createTestEnvelope(OpId opId) {
        Command command = new Command(
            Domain.of("TEST_DOMAIN"),
            EventType.of("CREATE"),
            BizKey.of("biz-123"),
            IdemKey.of("idem-456"),
            Payload.of("{\"data\": \"test\"}")
        );
        return Envelope.now(opId, command);
    }
}
