package ru.course.monitoring;

public record NetworkMetrics(
        String interfaceName,
        long rxBytes,
        long rxPackets,
        long rxErrors,
        long txBytes,
        long txPackets,
        long txErrors
) {
}