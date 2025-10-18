package com.ryuqq.orchestrator.core.protection.noop;

import com.ryuqq.orchestrator.core.model.OpId;
import com.ryuqq.orchestrator.core.protection.Bulkhead;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * NoOpBulkhead 유닛 테스트.
 *
 * @author Orchestrator Team
 * @since 1.0.0
 */
@DisplayName("NoOpBulkhead 테스트")
class NoOpBulkheadTest {

    @Test
    @DisplayName("tryAcquire() 는 항상 true를 반환한다")
    void tryAcquire_항상_true_반환() {
        // given
        Bulkhead bulkhead = new NoOpBulkhead();
        OpId opId = OpId.of("test-op");

        // when
        boolean result = bulkhead.tryAcquire(opId);

        // then
        assertTrue(result);
    }

    @Test
    @DisplayName("tryAcquire(timeout) 은 항상 true를 반환한다")
    void tryAcquireWithTimeout_항상_true_반환() throws InterruptedException {
        // given
        Bulkhead bulkhead = new NoOpBulkhead();
        OpId opId = OpId.of("test-op");

        // when
        boolean result = bulkhead.tryAcquire(opId, 1000);

        // then
        assertTrue(result);
    }

    @Test
    @DisplayName("release() 는 예외 없이 실행된다")
    void release_예외_없이_실행() {
        // given
        Bulkhead bulkhead = new NoOpBulkhead();
        OpId opId = OpId.of("test-op");

        // when & then
        assertDoesNotThrow(() -> bulkhead.release(opId));
    }

    @Test
    @DisplayName("getCurrentConcurrency() 는 항상 0을 반환한다")
    void getCurrentConcurrency_항상_0_반환() {
        // given
        Bulkhead bulkhead = new NoOpBulkhead();

        // when
        int concurrency = bulkhead.getCurrentConcurrency();

        // then
        assertEquals(0, concurrency);
    }

    @Test
    @DisplayName("getConfig() 는 무제한 설정을 반환한다")
    void getConfig_무제한_설정_반환() {
        // given
        Bulkhead bulkhead = new NoOpBulkhead();

        // when
        var config = bulkhead.getConfig();

        // then
        assertNotNull(config);
        assertEquals(Integer.MAX_VALUE, config.maxConcurrentCalls());
        assertEquals(0, config.maxWaitDurationMs());
    }

    @Test
    @DisplayName("acquire와 release를 반복해도 항상 동일한 동작을 한다")
    void acquire_release_반복_동일_동작() throws InterruptedException {
        // given
        Bulkhead bulkhead = new NoOpBulkhead();
        OpId opId = OpId.of("test-op");

        // when & then
        for (int i = 0; i < 100; i++) {
            assertTrue(bulkhead.tryAcquire(opId));
            assertTrue(bulkhead.tryAcquire(opId, 100));
            assertDoesNotThrow(() -> bulkhead.release(opId));
            assertEquals(0, bulkhead.getCurrentConcurrency());
        }
    }
}
