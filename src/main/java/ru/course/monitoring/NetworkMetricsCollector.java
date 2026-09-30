package ru.course.monitoring;

import java.io.IOException;
import java.util.List;

public interface NetworkMetricsCollector {

    List<NetworkMetrics> collect() throws IOException;

}