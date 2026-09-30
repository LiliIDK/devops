package ru.course.monitoring;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class LinuxNetworkMetricsCollector implements NetworkMetricsCollector {

    private static final Path PROC_NET_DEV = Path.of("/proc/net/dev");

    @Override
    public List<NetworkMetrics> collect() throws IOException {

        if (!Files.exists(PROC_NET_DEV)) {
            throw new IOException(
                    "File /proc/net/dev not found. " +
                            "Linux network metrics are unavailable on this operating system."
            );
        }

        List<String> lines = Files.readAllLines(PROC_NET_DEV);
        List<NetworkMetrics> metrics = new ArrayList<>();

        for (String line : lines) {

            line = line.trim();

            // Пропускаем заголовки
            if (line.startsWith("Inter-") || line.startsWith("face")) {
                continue;
            }

            if (!line.contains(":")) {
                continue;
            }

            String[] interfaceSplit = line.split(":", 2);

            String interfaceName = interfaceSplit[0].trim();

            String[] values = interfaceSplit[1]
                    .trim()
                    .split("\\s+");

            /*
             * Формат /proc/net/dev:
             *
             * Receive:
             * 0 bytes
             * 1 packets
             * 2 errs
             * 3 drop
             * ...
             *
             * Transmit:
             * 8 bytes
             * 9 packets
             * 10 errs
             * 11 drop
             * ...
             */

            long rxBytes = Long.parseLong(values[0]);
            long rxPackets = Long.parseLong(values[1]);
            long rxErrors = Long.parseLong(values[2]);

            long txBytes = Long.parseLong(values[8]);
            long txPackets = Long.parseLong(values[9]);
            long txErrors = Long.parseLong(values[10]);

            NetworkMetrics networkMetrics = new NetworkMetrics(
                    interfaceName,
                    rxBytes,
                    rxPackets,
                    rxErrors,
                    txBytes,
                    txPackets,
                    txErrors
            );

            metrics.add(networkMetrics);
        }

        return metrics;
    }
}