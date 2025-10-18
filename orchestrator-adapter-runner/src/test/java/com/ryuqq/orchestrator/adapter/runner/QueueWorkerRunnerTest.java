package com.ryuqq.orchestrator.adapter.runner;

import com.ryuqq.orchestrator.core.contract.Command;
import com.ryuqq.orchestrator.core.contract.Envelope;
import com.ryuqq.orchestrator.core.executor.Executor;
import com.ryuqq.orchestrator.core.model.*;
import com.ryuqq.orchestrator.core.outcome.Fail;
import com.ryuqq.orchestrator.core.outcome.Ok;
import com.ryuqq.orchestrator.core.outcome.Retry;
import com.ryuqq.orchestrator.core.spi.Bus;
import com.ryuqq.orchestrator.core.spi.Store;
import com.ryuqq.orchestrator.core.statemachine.OperationState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * QueueWorkerRunner 유닛 테스트.
 *
 * <p>QueueWorkerRunner의 핵심 동작을 검증합니다:</p>
 * <ul>
 *   <li>Ok 처리: writeAhead → finalize 순서 보장</li>
 *   <li>Retry 처리: RetryBudget 확인 및 backoff 적용</li>
 *   <li>Fail 처리: 즉시 finalize(FAILED)</li>
 *   <li>예외 처리: NACK 및 재시도</li>
 * </ul>
 *
 * @author Orchestrator Team
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
class QueueWorkerRunnerTest {

    @Mock
    private Bus bus;

    @Mock
    private Store store;

    @Mock
    private Executor executor;

    private QueueWorkerRunner runner;
    private QueueWorkerConfig config;
    private Envelope testEnvelope;
    private OpId testOpId;

    @BeforeEach
    void setUp() {
        config = new QueueWorkerConfig(); // Already has maxRetries=3 and dlqEnabled=true by default

        runner = new QueueWorkerRunner(bus, store, executor, config);

        testOpId = OpId.of("test-op-123");
        Command testCommand = new Command(
            Domain.of("TEST_DOMAIN"),
            EventType.of("CREATE"),
            BizKey.of("biz-123"),
            IdemKey.of("idem-456"),
            Payload.of("{\"data\": \"test\"}")
        );
        testEnvelope = Envelope.now(testOpId, testCommand);
    }

    /**
     * pump() 호출 후 비동기 작업 완료를 대기하는 헬퍼 메서드.
     * ThreadPool을 사용하므로 작업 완료를 기다려야 함.
     */
    private void pumpAndWait() throws InterruptedException {
        runner.pump();
        Thread.sleep(200); // 비동기 작업 완료 대기 (배치 처리 고려)
    }

    // ============================================================
    // 1. Ok 처리: writeAhead → finalize 순서 보장
    // ============================================================

    @Test
    void pump_Ok_처리_시_writeAhead_후_finalize_순서로_호출됨() throws InterruptedException {
        // given
        when(bus.dequeue(anyInt())).thenReturn(List.of(testEnvelope));

        // Executor가 즉시 COMPLETED 상태로 반환
        doAnswer(invocation -> {
            when(executor.getState(testOpId)).thenReturn(OperationState.COMPLETED);
            when(executor.getOutcome(testOpId)).thenReturn(new Ok(testOpId, "Success"));
            return null;
        }).when(executor).execute(testEnvelope);

        // when
        pumpAndWait();

        // then
        InOrder inOrder = inOrder(store, bus);
        inOrder.verify(store).writeAhead(eq(testOpId), any(Ok.class));
        inOrder.verify(store).finalize(testOpId, OperationState.COMPLETED);
        inOrder.verify(bus).ack(testEnvelope);
    }

    @Test
    void pump_Ok_처리_시_finalize_실패해도_ACK_처리됨() throws InterruptedException {
        // given
        when(bus.dequeue(anyInt())).thenReturn(List.of(testEnvelope));

        doAnswer(invocation -> {
            when(executor.getState(testOpId)).thenReturn(OperationState.COMPLETED);
            when(executor.getOutcome(testOpId)).thenReturn(new Ok(testOpId, "Success"));
            return null;
        }).when(executor).execute(testEnvelope);

        // finalize 실패 시뮬레이션 (Finalizer가 복구할 것임)
        doThrow(new RuntimeException("DB error")).when(store)
            .finalize(testOpId, OperationState.COMPLETED);

        // when
        pumpAndWait();

        // then
        verify(store).writeAhead(eq(testOpId), any(Ok.class));
        verify(store).finalize(testOpId, OperationState.COMPLETED);
        verify(bus).ack(testEnvelope); // finalize 실패해도 ACK
    }

    // ============================================================
    // 2. Retry 처리: RetryBudget 확인 및 backoff 적용
    // ============================================================

    @Test
    void pump_Retry_처리_시_RetryBudget_남아있으면_재게시됨() throws InterruptedException {
        // given
        when(bus.dequeue(anyInt())).thenReturn(List.of(testEnvelope));

        Retry retry = new Retry("NetworkTimeout", 1, 1000);
        doAnswer(invocation -> {
            when(executor.getState(testOpId)).thenReturn(OperationState.COMPLETED);
            when(executor.getOutcome(testOpId)).thenReturn(retry);
            return null;
        }).when(executor).execute(testEnvelope);

        // when
        pumpAndWait();

        // then
        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        verify(bus).publish(eq(testEnvelope), delayCaptor.capture());

        // Backoff 적용 확인 (attemptCount=1 → BASE_DELAY * 2^(1-1) = 1000ms + jitter)
        // jitter = 0 ~ 1000 * 0.1 = 0 ~ 100ms
        long delay = delayCaptor.getValue();
        assertThat(delay).isGreaterThanOrEqualTo(1000);  // 최소 1초
        assertThat(delay).isLessThan(1100);              // 1초 + 10% jitter (100ms)

        verify(bus).ack(testEnvelope);
        verify(store, never()).finalize(any(), any()); // finalize 호출 안 됨
    }

    @Test
    void pump_Retry_처리_시_RetryBudget_소진되면_finalize_FAILED_호출됨() throws InterruptedException {
        // given
        when(bus.dequeue(anyInt())).thenReturn(List.of(testEnvelope));

        // maxRetries=3, attemptCount=3 (예산 소진)
        Retry retry = new Retry("NetworkTimeout", 3, 4000);
        doAnswer(invocation -> {
            when(executor.getState(testOpId)).thenReturn(OperationState.COMPLETED);
            when(executor.getOutcome(testOpId)).thenReturn(retry);
            return null;
        }).when(executor).execute(testEnvelope);

        // when
        pumpAndWait();

        // then
        verify(bus, never()).publish(any(), anyLong()); // 재게시 안 됨
        verify(store).finalize(testOpId, OperationState.FAILED);
        verify(bus).ack(testEnvelope);
    }

    // ============================================================
    // 3. Fail 처리: 즉시 finalize(FAILED)
    // ============================================================

    @Test
    void pump_Fail_처리_시_즉시_finalize_FAILED_호출됨() throws InterruptedException {
        // given
        when(bus.dequeue(anyInt())).thenReturn(List.of(testEnvelope));

        Fail fail = Fail.of("INVALID_INPUT", "Invalid data format");
        doAnswer(invocation -> {
            when(executor.getState(testOpId)).thenReturn(OperationState.COMPLETED);
            when(executor.getOutcome(testOpId)).thenReturn(fail);
            return null;
        }).when(executor).execute(testEnvelope);

        // when
        pumpAndWait();

        // then
        verify(store).finalize(testOpId, OperationState.FAILED);
        verify(bus).publishToDLQ(testEnvelope, fail);
        verify(bus).ack(testEnvelope);
    }

    @Test
    void pump_Fail_처리_시_DLQ_비활성화_상태면_DLQ_전송_안_됨() throws InterruptedException {
        // given
        QueueWorkerConfig configWithDlqDisabled = config.withDlqEnabled(false);
        runner = new QueueWorkerRunner(bus, store, executor, configWithDlqDisabled);

        when(bus.dequeue(anyInt())).thenReturn(List.of(testEnvelope));

        Fail fail = Fail.of("INVALID_INPUT", "Invalid data format");
        doAnswer(invocation -> {
            when(executor.getState(testOpId)).thenReturn(OperationState.COMPLETED);
            when(executor.getOutcome(testOpId)).thenReturn(fail);
            return null;
        }).when(executor).execute(testEnvelope);

        // when
        pumpAndWait();

        // then
        verify(store).finalize(testOpId, OperationState.FAILED);
        verify(bus, never()).publishToDLQ(any(), any());
        verify(bus).ack(testEnvelope);
    }

    // ============================================================
    // 4. 예외 처리: NACK 및 재시도
    // ============================================================

    @Test
    void pump_처리_중_예외_발생_시_NACK_호출됨() throws InterruptedException {
        // given
        when(bus.dequeue(anyInt())).thenReturn(List.of(testEnvelope));
        doThrow(new RuntimeException("Executor error")).when(executor).execute(testEnvelope);

        // when
        pumpAndWait();

        // then: 비동기 실행으로 인해 예외는 워커 스레드에서 발생하고,
        // RuntimeException이 throw되어 processEnvelope의 catch 블록에서 NACK 처리됨
        verify(bus).nack(testEnvelope);
        verify(bus, never()).ack(any());
    }

    // ============================================================
    // 5. 배치 처리
    // ============================================================

    @Test
    void pump_여러_Envelope_배치_처리_가능() throws InterruptedException {
        // given
        OpId opId1 = OpId.of("op-1");
        OpId opId2 = OpId.of("op-2");
        Envelope env1 = Envelope.now(opId1, testEnvelope.command());
        Envelope env2 = Envelope.now(opId2, testEnvelope.command());

        when(bus.dequeue(anyInt())).thenReturn(List.of(env1, env2));

        // 두 작업 모두 Ok로 완료
        doAnswer(invocation -> {
            Envelope env = invocation.getArgument(0);
            OpId opId = env.opId();
            when(executor.getState(opId)).thenReturn(OperationState.COMPLETED);
            when(executor.getOutcome(opId)).thenReturn(new Ok(opId, "Success"));
            return null;
        }).when(executor).execute(any());

        // when
        pumpAndWait();

        // then
        verify(executor, times(2)).execute(any());
        verify(store, times(2)).writeAhead(any(), any());
        verify(store, times(2)).finalize(any(), eq(OperationState.COMPLETED));
        verify(bus, times(2)).ack(any());
    }

    // ============================================================
    // 6. 생성자 유효성 검증
    // ============================================================

    @Test
    void 생성자_bus가_null이면_예외() {
        assertThatThrownBy(() -> new QueueWorkerRunner(null, store, executor, config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("bus cannot be null");
    }

    @Test
    void 생성자_store가_null이면_예외() {
        assertThatThrownBy(() -> new QueueWorkerRunner(bus, null, executor, config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("store cannot be null");
    }

    @Test
    void 생성자_executor가_null이면_예외() {
        assertThatThrownBy(() -> new QueueWorkerRunner(bus, store, null, config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("executor cannot be null");
    }

    @Test
    void 생성자_config가_null이면_예외() {
        assertThatThrownBy(() -> new QueueWorkerRunner(bus, store, executor, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("config cannot be null");
    }
}
