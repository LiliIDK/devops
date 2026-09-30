package ru.course.monitoring;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class VictoriaMetricsClient {

    private final HttpClient httpClient;
    private final String url;

    public VictoriaMetricsClient(String url) {

        this.url = url;

        this.httpClient =
                HttpClient.newBuilder()
                        .connectTimeout(
                                Duration.ofSeconds(5)
                        )
                        .build();
    }

    public void send(String payload)
            throws IOException, InterruptedException {

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(10))
                        .header(
                                "Content-Type",
                                "text/plain"
                        )
                        .POST(
                                HttpRequest.BodyPublishers
                                        .ofString(payload)
                        )
                        .build();

        HttpResponse<String> response =
                httpClient.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        if (
                response.statusCode() < 200
                        ||
                        response.statusCode() >= 300
        ) {

            throw new IOException(
                    "VictoriaMetrics returned HTTP "
                            + response.statusCode()
                            + ": "
                            + response.body()
            );
        }
    }
}