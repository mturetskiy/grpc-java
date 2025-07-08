package dev.teapod.exp.grpc.stats;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class ProcessingStats {
    private int totalMessages;
    private double endToEndMeanLatency;
    private double endToEndP90Latency;
    private double reqTransportMeanLatency;
    private double reqTransportP90Latency;
    private double respTransportMeanLatency;
    private double respTransportP90Latency;
    private double totalDurationMs;
    private double throughput;

    public void printStats() {
        log.info("Result stats:\n\ttotal messages: {}\n\ttotalTime: {} ms" +
                        "\n\tendToEnd mean latency: {} micros\n\tendToEnd p90 latency: {} micros" +
                        "\n\trequestTransport mean latency: {} micros\n\trequestTransport p90 latency: {} micros" +
                        "\n\tresponseTransport mean latency: {} micros\n\tresponseTransport p90 latency: {} micros" +
                        "\n\tthroughput: {} msg/sec",
                totalMessages, (float) totalDurationMs,
                (float) (endToEndMeanLatency / 1000.0), (float) (endToEndP90Latency / 1000.0),
                (float) (reqTransportMeanLatency / 1000.0), (float) (reqTransportP90Latency / 1000.0),
                (float) (respTransportMeanLatency / 1000.0), (float) (respTransportP90Latency / 1000.0),
                (float) throughput);
    }
}
