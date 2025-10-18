package com.ryuqq.orchestrator.adapter.runner;

import com.ryuqq.orchestrator.core.model.OpId;
import com.ryuqq.orchestrator.core.outcome.Fail;
import com.ryuqq.orchestrator.core.outcome.Ok;
import com.ryuqq.orchestrator.core.outcome.Retry;
import com.ryuqq.orchestrator.core.spi.Store;
import com.ryuqq.orchestrator.core.spi.WriteAheadState;
import com.ryuqq.orchestrator.core.statemachine.OperationState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Finalizer 유닛 테스트.
 *
 * <p>Finalizer의 복구 동작을 검증합니다:</p>
 * <ul>
 *   <li>PENDING 항목 스캔 및 복구</li>
 *   <li>Ok → finalize(COMPLETED)</li>
 *   <li>Fail → finalize(FAILED)</li>
 *   <li>Retry → finalize(FAILED) (이상 케이스)</li>
 *   <li>예외 발생 시에도 계속 진행</li>
 * </ul>
 *
 * @author Orchestrator Team
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
class FinalizerTest {

    @Mock
    private Store store;

    private Finalizer finalizer;
    private FinalizerConfig config;

    @BeforeEach
    void setUp() {
        config = new FinalizerConfig().withBatchSize(100);
        finalizer = new Finalizer(store, config);
    }

    // ============================================================
    // 1. PENDING 항목 복구: Ok → COMPLETED
    // ============================================================

    @Test
    void scan_Ok_outcome이_PENDING_상태면_finalize_COMPLETED_호출됨() {
        // given
        OpId opId = OpId.of("test-op-1");
        when(store.scanWA(WriteAheadState.PENDING, 100)).thenReturn(List.of(opId));
        when(store.getWriteAheadOutcome(opId)).thenReturn(new Ok(opId, "Success"));

        // when
        finalizer.scan();

        // then
        verify(store).getWriteAheadOutcome(opId);
        verify(store).finalize(opId, OperationState.COMPLETED);
    }

    // ============================================================
    // 2. PENDING 항목 복구: Fail → FAILED
    // ============================================================

    @Test
    void scan_Fail_outcome이_PENDING_상태면_finalize_FAILED_호출됨() {
        // given
        OpId opId = OpId.of("test-op-2");
        Fail fail = Fail.of("INVALID_INPUT", "Invalid data");

        when(store.scanWA(WriteAheadState.PENDING, 100)).thenReturn(List.of(opId));
        when(store.getWriteAheadOutcome(opId)).thenReturn(fail);

        // when
        finalizer.scan();

        // then
        verify(store).getWriteAheadOutcome(opId);
        verify(store).finalize(opId, OperationState.FAILED);
    }

    // ============================================================
    // 3. PENDING 항목 복구: Retry → FAILED (이상 케이스)
    // ============================================================

    @Test
    void scan_Retry_outcome이_PENDING_상태면_finalize_FAILED_호출됨_경고_로그() {
        // given
        OpId opId = OpId.of("test-op-3");
        Retry retry = new Retry("NetworkTimeout", 1, 1000);

        when(store.scanWA(WriteAheadState.PENDING, 100)).thenReturn(List.of(opId));
        when(store.getWriteAheadOutcome(opId)).thenReturn(retry);

        // when
        finalizer.scan();

        // then
        verify(store).getWriteAheadOutcome(opId);
        verify(store).finalize(opId, OperationState.FAILED);
        // 로그에 "Unexpected Retry" 경고가 출력될 것임 (수동 확인)
    }

    // ============================================================
    // 4. 여러 항목 배치 복구
    // ============================================================

    @Test
    void scan_여러_PENDING_항목을_배치로_복구() {
        // given
        OpId opId1 = OpId.of("op-1");
        OpId opId2 = OpId.of("op-2");
        OpId opId3 = OpId.of("op-3");

        when(store.scanWA(WriteAheadState.PENDING, 100))
            .thenReturn(List.of(opId1, opId2, opId3));

        when(store.getWriteAheadOutcome(opId1)).thenReturn(new Ok(opId1, "Success"));
        when(store.getWriteAheadOutcome(opId2)).thenReturn(Fail.of("ERR", "Error"));
        when(store.getWriteAheadOutcome(opId3)).thenReturn(new Ok(opId3, "Success"));

        // when
        finalizer.scan();

        // then
        verify(store).finalize(opId1, OperationState.COMPLETED);
        verify(store).finalize(opId2, OperationState.FAILED);
        verify(store).finalize(opId3, OperationState.COMPLETED);
    }

    // ============================================================
    // 5. 예외 처리: 일부 실패해도 계속 진행
    // ============================================================

    @Test
    void scan_일부_항목_finalize_실패해도_다른_항목_계속_처리됨() {
        // given
        OpId opId1 = OpId.of("op-1");
        OpId opId2 = OpId.of("op-2");
        OpId opId3 = OpId.of("op-3");

        when(store.scanWA(WriteAheadState.PENDING, 100))
            .thenReturn(List.of(opId1, opId2, opId3));

        when(store.getWriteAheadOutcome(opId1)).thenReturn(new Ok(opId1, "Success"));
        when(store.getWriteAheadOutcome(opId2)).thenReturn(new Ok(opId2, "Success"));
        when(store.getWriteAheadOutcome(opId3)).thenReturn(new Ok(opId3, "Success"));

        // opId2 finalize 실패 시뮬레이션
        doThrow(new RuntimeException("DB error"))
            .when(store).finalize(eq(opId2), any());

        // when
        finalizer.scan();

        // then
        verify(store).finalize(opId1, OperationState.COMPLETED);
        verify(store).finalize(opId2, OperationState.COMPLETED); // 시도는 했음
        verify(store).finalize(opId3, OperationState.COMPLETED); // 계속 진행됨
    }

    @Test
    void scan_getWriteAheadOutcome_실패해도_다른_항목_계속_처리됨() {
        // given
        OpId opId1 = OpId.of("op-1");
        OpId opId2 = OpId.of("op-2");
        OpId opId3 = OpId.of("op-3");

        when(store.scanWA(WriteAheadState.PENDING, 100))
            .thenReturn(List.of(opId1, opId2, opId3));

        when(store.getWriteAheadOutcome(opId1)).thenReturn(new Ok(opId1, "Success"));
        when(store.getWriteAheadOutcome(opId2)).thenThrow(new RuntimeException("Not found"));
        when(store.getWriteAheadOutcome(opId3)).thenReturn(new Ok(opId3, "Success"));

        // when
        finalizer.scan();

        // then
        verify(store).finalize(opId1, OperationState.COMPLETED);
        verify(store, never()).finalize(eq(opId2), any()); // opId2는 스킵됨
        verify(store).finalize(opId3, OperationState.COMPLETED); // 계속 진행됨
    }

    // ============================================================
    // 6. PENDING 항목이 없는 경우
    // ============================================================

    @Test
    void scan_PENDING_항목이_없으면_finalize_호출_안_됨() {
        // given
        when(store.scanWA(WriteAheadState.PENDING, 100)).thenReturn(List.of());

        // when
        finalizer.scan();

        // then
        verify(store).scanWA(WriteAheadState.PENDING, 100);
        verify(store, never()).getWriteAheadOutcome(any());
        verify(store, never()).finalize(any(), any());
    }

    // ============================================================
    // 7. 생성자 유효성 검증
    // ============================================================

    @Test
    void 생성자_store가_null이면_예외() {
        assertThatThrownBy(() -> new Finalizer(null, config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("store cannot be null");
    }

    @Test
    void 생성자_config가_null이면_예외() {
        assertThatThrownBy(() -> new Finalizer(store, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("config cannot be null");
    }
}
