package com.ryuqq.orchestrator.core.protection;

/**
 * Bulkhead 설정.
 *
 * <p>Bulkhead의 동작을 제어하는 설정 정보입니다.</p>
 *
 * <p>Java 21 record를 사용하여 불변성을 보장합니다.</p>
 *
 * @param maxConcurrentCalls 최대 동시 실행 수 (예: 10)
 * @param maxWaitDurationMs 최대 대기 시간 (밀리초)
 * @author Orchestrator Team
 * @since 1.0.0
 */
public record BulkheadConfig(int maxConcurrentCalls, int maxWaitDurationMs) {

    /**
     * Compact constructor with validation.
     *
     * @throws IllegalArgumentException if maxConcurrentCalls is not positive
     * @throws IllegalArgumentException if maxWaitDurationMs is negative
     */
    public BulkheadConfig {
        if (maxConcurrentCalls <= 0) {
            throw new IllegalArgumentException("maxConcurrentCalls must be positive");
        }
        if (maxWaitDurationMs < 0) {
            throw new IllegalArgumentException("maxWaitDurationMs cannot be negative");
        }
    }
}
