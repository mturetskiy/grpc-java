package dev.teapod.exp.grpc.stats;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class ProcessingStats {
    private int totalMessages;
    private double mean;
    private double p90;
    private double totalDurationMs;
    private double throughput;

    public void printStats() {
        log.info("Result stats:\n\ttotal messages: {}\n\ttotalTime: {} ms" +
                        "\n\tmean latency: {} micros\n\tp90 latency: {} micros\n\tthroughput: {} msg/sec",
                totalMessages, (float) totalDurationMs,
                (float) (mean / 1000.0), (float) (p90 / 1000.0), (float) throughput);
    }
}
