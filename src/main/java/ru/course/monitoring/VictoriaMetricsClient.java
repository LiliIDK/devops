package ru.course.monitoring;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class VictoriaMetricsClient {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final String url;
    private final String agentName;

    public VictoriaMetricsClient(String url, String agentName) {
        this.url = url;
        this.agentName = agentName;
    }

    public void send(List<NetworkMetrics> metrics)
            throws IOException, InterruptedException {

        StringBuilder body = new StringBuilder();

        for (NetworkMetrics metric : metrics) {

            String labels =
                    "{agent=\"" + agentName +
                            "\",interface=\"" + metric.interfaceName() + "\"}";

            body.append("network_rx_bytes")
                    .append(labels)
                    .append(" ")
                    .append(metric.rxBytes())
                    .append("\n");

            body.append("network_tx_bytes")
                    .append(labels)
                    .append(" ")
                    .append(metric.txBytes())
                    .append("\n");

            body.append("network_rx_packets")
                    .append(labels)
                    .append(" ")
                    .append(metric.rxPackets())
                    .append("\n");

            body.append("network_tx_packets")
                    .append(labels)
                    .append(" ")
                    .append(metric.txPackets())
                    .append("\n");

            body.append("network_rx_errors")
                    .append(labels)
                    .append(" ")
                    .append(metric.rxErrors())
                    .append("\n");

            body.append("network_tx_errors")
                    .append(labels)
                    .append(" ")
                    .append(metric.txErrors())
                    .append("\n");
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        HttpResponse<String> response =
                httpClient.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        if (response.statusCode() < 200 ||
                response.statusCode() >= 300) {

            throw new IOException(
                    "VictoriaMetrics returned HTTP "
                            + response.statusCode()
            );
        }
    }
}