package ru.course.monitoring;

import java.util.List;

public class PrometheusPayloadBuilder {

    public String build(
            List<NetworkMetrics> metrics,
            String agentName,
            long timestampMillis
    ) {

        StringBuilder body = new StringBuilder();

        for (NetworkMetrics metric : metrics) {

            String labels =
                    "{agent=\"" + escape(agentName) +
                            "\",interface=\"" + escape(metric.interfaceName()) + "\"}";

            appendMetric(
                    body,
                    "network_rx_bytes",
                    labels,
                    metric.rxBytes(),
                    timestampMillis
            );

            appendMetric(
                    body,
                    "network_tx_bytes",
                    labels,
                    metric.txBytes(),
                    timestampMillis
            );

            appendMetric(
                    body,
                    "network_rx_packets",
                    labels,
                    metric.rxPackets(),
                    timestampMillis
            );

            appendMetric(
                    body,
                    "network_tx_packets",
                    labels,
                    metric.txPackets(),
                    timestampMillis
            );

            appendMetric(
                    body,
                    "network_rx_errors",
                    labels,
                    metric.rxErrors(),
                    timestampMillis
            );

            appendMetric(
                    body,
                    "network_tx_errors",
                    labels,
                    metric.txErrors(),
                    timestampMillis
            );
        }

        return body.toString();
    }

    private void appendMetric(
            StringBuilder body,
            String metricName,
            String labels,
            long value,
            long timestampMillis
    ) {

        body.append(metricName)
                .append(labels)
                .append(" ")
                .append(value)
                .append(" ")
                .append(timestampMillis)
                .append("\n");
    }

    private String escape(String value) {

        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n");
    }
}