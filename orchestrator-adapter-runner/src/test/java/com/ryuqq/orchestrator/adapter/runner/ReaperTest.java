package com.ryuqq.orchestrator.adapter.runner;

import com.ryuqq.orchestrator.core.contract.Command;
import com.ryuqq.orchestrator.core.contract.Envelope;
import com.ryuqq.orchestrator.core.model.*;
import com.ryuqq.orchestrator.core.spi.Bus;
import com.ryuqq.orchestrator.core.spi.Store;
import com.ryuqq.orchestrator.core.statemachine.OperationState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Reaper 유닛 테스트.
 *
 * <p>Reaper의 장기 IN_PROGRESS 작업 리컨실 동작을 검증합니다:</p>
 * <ul>
 *   <li>RETRY 전략: Envelope 재게시</li>
 *   <li>FAIL 전략: finalize(FAILED)</li>
 *   <li>타임아웃 임계값 검증</li>
 *   <li>배치 리컨실 처리</li>
 *   <li>예외 발생 시에도 계속 진행</li>
 * </ul>
 *
 * @author Orchestrator Team
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
class ReaperTest {

    @Mock
    private Bus bus;

    @Mock
    private Store store;

    private Reaper reaper;
    private ReaperConfig config;

    @BeforeEach
    void setUp() {
        config = new ReaperConfig(); // Already has timeoutThresholdMs=600000, batchSize=50, defaultStrategy=FAIL by default

        reaper = new Reaper(bus, store, config);
    }

    // ============================================================
    // 1. RETRY 전략: Envelope 재게시
    // ============================================================

    @Test
    void scan_RETRY_전략이면_장기_IN_PROGRESS_작업을_재게시함() {
        // given
        ReaperConfig configWithRetry = config.withDefaultStrategy(ReconcileStrategy.RETRY);
        reaper = new Reaper(bus, store, configWithRetry);

        OpId opId = OpId.of("stuck-op-1");
        Envelope envelope = createTestEnvelope(opId);

        when(store.scanInProgress(600000, 50)).thenReturn(List.of(opId));
        when(store.getEnvelope(opId)).thenReturn(envelope);

        // when
        reaper.scan();

        // then
        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        verify(store).scanInProgress(600000, 50);
        verify(store).getEnvelope(opId);
        verify(bus).publish(eq(envelope), delayCaptor.capture());

        // 지연 없이 즉시 재게시 (delay = 0)
        assertThat(delayCaptor.getValue()).isEqualTo(0L);
    }

    @Test
    void scan_RETRY_전략_시_여러_작업_재게시_가능() {
        // given
        ReaperConfig configWithRetry = config.withDefaultStrategy(ReconcileStrategy.RETRY);
        reaper = new Reaper(bus, store, configWithRetry);

        OpId opId1 = OpId.of("stuck-op-1");
        OpId opId2 = OpId.of("stuck-op-2");
        OpId opId3 = OpId.of("stuck-op-3");

        Envelope env1 = createTestEnvelope(opId1);
        Envelope env2 = createTestEnvelope(opId2);
        Envelope env3 = createTestEnvelope(opId3);

        when(store.scanInProgress(600000, 50)).thenReturn(List.of(opId1, opId2, opId3));
        when(store.getEnvelope(opId1)).thenReturn(env1);
        when(store.getEnvelope(opId2)).thenReturn(env2);
        when(store.getEnvelope(opId3)).thenReturn(env3);

        // when
        reaper.scan();

        // then
        verify(bus).publish(env1, 0);
        verify(bus).publish(env2, 0);
        verify(bus).publish(env3, 0);
        verify(store, never()).finalize(any(), any());
    }

    // ============================================================
    // 2. FAIL 전략: finalize(FAILED)
    // ============================================================

    @Test
    void scan_FAIL_전략이면_장기_IN_PROGRESS_작업을_FAILED로_종료함() {
        // given (default strategy = FAIL)
        OpId opId = OpId.of("stuck-op-1");

        when(store.scanInProgress(600000, 50)).thenReturn(List.of(opId));

        // when
        reaper.scan();

        // then
        verify(store).scanInProgress(600000, 50);
        verify(store, never()).getEnvelope(any()); // FAIL 전략은 envelope 조회 안 함
        verify(store).finalize(opId, OperationState.FAILED);
        verify(bus, never()).publish(any(), anyLong());
    }

    @Test
    void scan_FAIL_전략_시_여러_작업_FAILED로_종료_가능() {
        // given
        OpId opId1 = OpId.of("stuck-op-1");
        OpId opId2 = OpId.of("stuck-op-2");
        OpId opId3 = OpId.of("stuck-op-3");

        when(store.scanInProgress(600000, 50)).thenReturn(List.of(opId1, opId2, opId3));

        // when
        reaper.scan();

        // then
        verify(store).finalize(opId1, OperationState.FAILED);
        verify(store).finalize(opId2, OperationState.FAILED);
        verify(store).finalize(opId3, OperationState.FAILED);
        verify(store, never()).getEnvelope(any()); // FAIL 전략은 envelope 조회 안 함
        verify(bus, never()).publish(any(), anyLong());
    }

    // ============================================================
    // 3. 타임아웃 임계값 검증
    // ============================================================

    @Test
    void scan_설정된_타임아웃_임계값으로_scanInProgress_호출됨() {
        // given
        ReaperConfig configWith30MinTimeout = config.withTimeoutThresholdMs(1800000); // 30분
        reaper = new Reaper(bus, store, configWith30MinTimeout);

        when(store.scanInProgress(1800000, 50)).thenReturn(List.of());

        // when
        reaper.scan();

        // then
        verify(store).scanInProgress(1800000, 50);
    }

    @Test
    void scan_배치_크기_설정대로_scanInProgress_호출됨() {
        // given
        ReaperConfig configWithBatch100 = config.withBatchSize(100);
        reaper = new Reaper(bus, store, configWithBatch100);

        when(store.scanInProgress(600000, 100)).thenReturn(List.of());

        // when
        reaper.scan();

        // then
        verify(store).scanInProgress(600000, 100);
    }

    // ============================================================
    // 4. 예외 처리: 일부 실패해도 계속 진행
    // ============================================================

    @Test
    void scan_일부_작업_리컨실_실패해도_다른_작업_계속_처리됨() {
        // given
        OpId opId1 = OpId.of("stuck-op-1");
        OpId opId2 = OpId.of("stuck-op-2");
        OpId opId3 = OpId.of("stuck-op-3");

        when(store.scanInProgress(600000, 50)).thenReturn(List.of(opId1, opId2, opId3));
        when(store.getEnvelope(opId1)).thenReturn(createTestEnvelope(opId1));
        when(store.getEnvelope(opId2)).thenReturn(createTestEnvelope(opId2));
        when(store.getEnvelope(opId3)).thenReturn(createTestEnvelope(opId3));

        // opId2 finalize 실패 시뮬레이션
        doThrow(new RuntimeException("DB error"))
            .when(store).finalize(eq(opId2), any());

        // when
        reaper.scan();

        // then
        verify(store).finalize(opId1, OperationState.FAILED);
        verify(store).finalize(opId2, OperationState.FAILED); // 시도는 했음
        verify(store).finalize(opId3, OperationState.FAILED); // 계속 진행됨
    }

    @Test
    void scan_FAIL_전략_finalize_실패해도_다른_작업_계속_처리됨() {
        // given (default strategy = FAIL)
        OpId opId1 = OpId.of("stuck-op-1");
        OpId opId2 = OpId.of("stuck-op-2");
        OpId opId3 = OpId.of("stuck-op-3");

        when(store.scanInProgress(600000, 50)).thenReturn(List.of(opId1, opId2, opId3));

        // opId2 finalize 실패 시뮬레이션
        doThrow(new RuntimeException("DB error"))
            .when(store).finalize(eq(opId2), any());

        // when
        reaper.scan();

        // then
        verify(store).finalize(opId1, OperationState.FAILED);
        verify(store).finalize(opId2, OperationState.FAILED); // 시도는 했음
        verify(store).finalize(opId3, OperationState.FAILED); // 계속 진행됨
    }

    @Test
    void scan_RETRY_전략_시_publish_실패해도_다른_작업_계속_처리됨() {
        // given
        ReaperConfig configWithRetry = config.withDefaultStrategy(ReconcileStrategy.RETRY);
        reaper = new Reaper(bus, store, configWithRetry);

        OpId opId1 = OpId.of("stuck-op-1");
        OpId opId2 = OpId.of("stuck-op-2");
        OpId opId3 = OpId.of("stuck-op-3");

        Envelope env1 = createTestEnvelope(opId1);
        Envelope env2 = createTestEnvelope(opId2);
        Envelope env3 = createTestEnvelope(opId3);

        when(store.scanInProgress(600000, 50)).thenReturn(List.of(opId1, opId2, opId3));
        when(store.getEnvelope(opId1)).thenReturn(env1);
        when(store.getEnvelope(opId2)).thenReturn(env2);
        when(store.getEnvelope(opId3)).thenReturn(env3);

        // env2 publish 실패 시뮬레이션
        doThrow(new RuntimeException("Queue error"))
            .when(bus).publish(eq(env2), anyLong());

        // when
        reaper.scan();

        // then
        verify(bus).publish(env1, 0);
        verify(bus).publish(env2, 0); // 시도는 했음
        verify(bus).publish(env3, 0); // 계속 진행됨
    }

    // ============================================================
    // 5. 장기 IN_PROGRESS 작업이 없는 경우
    // ============================================================

    @Test
    void scan_장기_IN_PROGRESS_작업이_없으면_리컨실_안_됨() {
        // given
        when(store.scanInProgress(600000, 50)).thenReturn(List.of());

        // when
        reaper.scan();

        // then
        verify(store).scanInProgress(600000, 50);
        verify(store, never()).getEnvelope(any());
        verify(bus, never()).publish(any(), anyLong());
        verify(store, never()).finalize(any(), any());
    }

    // ============================================================
    // 6. 생성자 유효성 검증
    // ============================================================

    @Test
    void 생성자_bus가_null이면_예외() {
        assertThatThrownBy(() -> new Reaper(null, store, config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("bus cannot be null");
    }

    @Test
    void 생성자_store가_null이면_예외() {
        assertThatThrownBy(() -> new Reaper(bus, null, config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("store cannot be null");
    }

    @Test
    void 생성자_config가_null이면_예외() {
        assertThatThrownBy(() -> new Reaper(bus, store, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("config cannot be null");
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
