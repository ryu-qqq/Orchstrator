package com.ryuqq.orchestrator.core.protection;

/**
 * Rate Limiter 설정.
 *
 * <p>Rate Limiter의 동작을 제어하는 설정 정보입니다.</p>
 *
 * <p>Java 21 record를 사용하여 불변성을 보장합니다.</p>
 *
 * @param permitsPerSecond 초당 허용 요청 수 (예: 100.0)
 * @param maxBurstSize 버스트 허용량 (Token Bucket의 버킷 크기)
 * @author Orchestrator Team
 * @since 1.0.0
 */
public record RateLimiterConfig(double permitsPerSecond, int maxBurstSize) {

    /**
     * Compact constructor with validation.
     *
     * @throws IllegalArgumentException if permitsPerSecond is not positive
     * @throws IllegalArgumentException if maxBurstSize is not positive
     */
    public RateLimiterConfig {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        if (maxBurstSize <= 0) {
            throw new IllegalArgumentException("maxBurstSize must be positive");
        }
    }
}
