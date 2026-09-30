package ru.course.monitoring;

import java.io.IOException;
import java.util.List;

public class Main {

    private static final long COLLECT_INTERVAL_MS = 5000;

    public static void main(String[] args) {

        System.out.println("Monitoring Agent started!");

        String victoriaUrl = System.getenv()
                .getOrDefault(
                        "VICTORIA_URL",
                        "http://victoriametrics:8428/api/v1/import/prometheus"
                );

        String agentName = System.getenv()
                .getOrDefault(
                        "AGENT_NAME",
                        "agent-1"
                );

        NetworkMetricsCollector collector =
                new LinuxNetworkMetricsCollector();

        VictoriaMetricsClient victoriaMetricsClient =
                new VictoriaMetricsClient(
                        victoriaUrl,
                        agentName
                );

        while (true) {

            try {

                List<NetworkMetrics> metrics =
                        collector.collect();

                victoriaMetricsClient.send(metrics);

                System.out.println(
                        "Metrics sent successfully. Agent: "
                                + agentName
                );

            } catch (IOException | InterruptedException e) {

                System.out.println(
                        "Error: " + e.getMessage()
                );

            }

            try {

                Thread.sleep(COLLECT_INTERVAL_MS);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}