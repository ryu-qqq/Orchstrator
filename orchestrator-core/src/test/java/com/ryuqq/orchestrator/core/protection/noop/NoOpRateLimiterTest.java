package com.ryuqq.orchestrator.core.protection.noop;

import com.ryuqq.orchestrator.core.model.OpId;
import com.ryuqq.orchestrator.core.protection.RateLimiter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * NoOpRateLimiter 유닛 테스트.
 *
 * @author Orchestrator Team
 * @since 1.0.0
 */
@DisplayName("NoOpRateLimiter 테스트")
class NoOpRateLimiterTest {

    @Test
    @DisplayName("tryAcquire() 는 항상 true를 반환한다")
    void tryAcquire_항상_true_반환() {
        // given
        RateLimiter limiter = new NoOpRateLimiter();
        OpId opId = OpId.of("test-op");

        // when
        boolean result = limiter.tryAcquire(opId);

        // then
        assertTrue(result);
    }

    @Test
    @DisplayName("tryAcquire(timeout) 은 항상 true를 반환한다")
    void tryAcquireWithTimeout_항상_true_반환() throws InterruptedException {
        // given
        RateLimiter limiter = new NoOpRateLimiter();
        OpId opId = OpId.of("test-op");

        // when
        boolean result = limiter.tryAcquire(opId, 1000);

        // then
        assertTrue(result);
    }

    @Test
    @DisplayName("getConfig() 는 무제한 설정을 반환한다")
    void getConfig_무제한_설정_반환() {
        // given
        RateLimiter limiter = new NoOpRateLimiter();

        // when
        var config = limiter.getConfig();

        // then
        assertNotNull(config);
        assertEquals(Double.MAX_VALUE, config.permitsPerSecond());
        assertEquals(Integer.MAX_VALUE, config.maxBurstSize());
    }

    @Test
    @DisplayName("여러 번 호출해도 항상 true를 반환한다")
    void 여러번_호출_항상_true_반환() throws InterruptedException {
        // given
        RateLimiter limiter = new NoOpRateLimiter();
        OpId opId = OpId.of("test-op");

        // when & then
        for (int i = 0; i < 1000; i++) {
            assertTrue(limiter.tryAcquire(opId));
            assertTrue(limiter.tryAcquire(opId, 100));
        }
    }
}
