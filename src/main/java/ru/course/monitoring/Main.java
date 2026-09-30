package ru.course.monitoring;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) {

        String agentName =
                getEnv("AGENT_NAME", "agent-1");

        String rabbitHost =
                getEnv("RABBITMQ_HOST", "rabbitmq");

        int rabbitPort =
                Integer.parseInt(
                        getEnv("RABBITMQ_PORT", "5672")
                );

        String rabbitUser =
                getEnv("RABBITMQ_USER", "monitoring");

        String rabbitPassword =
                getEnv("RABBITMQ_PASSWORD", "monitoring123");

        String queueName =
                getEnv("RABBITMQ_QUEUE", "network.metrics");

        long collectInterval =
                Long.parseLong(
                        getEnv("COLLECT_INTERVAL_MS", "5000")
                );

        System.out.println(
                "Monitoring Agent started: " + agentName
        );

        System.out.println(
                "RabbitMQ: " + rabbitHost + ":" + rabbitPort
        );

        System.out.println(
                "Queue: " + queueName
        );

        NetworkMetricsCollector collector =
                new LinuxNetworkMetricsCollector();

        PrometheusPayloadBuilder payloadBuilder =
                new PrometheusPayloadBuilder();

        try (
                RabbitMqPublisher publisher =
                        new RabbitMqPublisher(
                                rabbitHost,
                                rabbitPort,
                                rabbitUser,
                                rabbitPassword,
                                queueName,
                                agentName
                        )
        ) {

            while (!Thread.currentThread().isInterrupted()) {

                try {

                    List<NetworkMetrics> metrics =
                            collector.collect();

                    long timestamp =
                            System.currentTimeMillis();

                    String payload =
                            payloadBuilder.build(
                                    metrics,
                                    agentName,
                                    timestamp
                            );

                    publisher.publish(payload);

                    System.out.println(
                            "Metrics published to RabbitMQ. Agent: "
                                    + agentName
                    );

                } catch (IOException | TimeoutException e) {

                    System.err.println(
                            "Publish error: "
                                    + e.getMessage()
                    );

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();
                    break;
                }

                try {

                    Thread.sleep(collectInterval);

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (IOException | TimeoutException e) {

            System.err.println(
                    "Cannot connect to RabbitMQ: "
                            + e.getMessage()
            );
        }
    }

    private static String getEnv(
            String name,
            String defaultValue
    ) {

        return System.getenv()
                .getOrDefault(name, defaultValue);
    }
}