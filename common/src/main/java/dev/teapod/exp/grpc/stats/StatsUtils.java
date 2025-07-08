package dev.teapod.exp.grpc.stats;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class StatsUtils {
    public static ProcessingStats collectProcessingStats(List<ClientProcessingStats> clientStats, int totalMessagesCount) {
        ProcessingStats processingStats = new ProcessingStats();
        DescriptiveStatistics endToEndStats = new DescriptiveStatistics(new double[totalMessagesCount]);
        DescriptiveStatistics requestStats = new DescriptiveStatistics(new double[totalMessagesCount]);
        DescriptiveStatistics responseStats = new DescriptiveStatistics(new double[totalMessagesCount]);

        AtomicLong maxTotalDurationNs = new AtomicLong(0);
        clientStats.forEach(cs -> {
            cs.getEndToEndDurations().forEach(durationNs -> {
                endToEndStats.addValue(Double.valueOf(durationNs));
            });

            cs.getRequestTransportDurations().forEach(durationNs -> {
                requestStats.addValue(Double.valueOf(durationNs));
            });

            cs.getResponseTransportDurations().forEach(durationNs -> {
                responseStats.addValue(Double.valueOf(durationNs));
            });

            long totalDurationNs = cs.getTotalDuration().toNanos();
            maxTotalDurationNs.updateAndGet(val -> Math.max(val, totalDurationNs));
        });

        processingStats.setTotalMessages(totalMessagesCount);
        processingStats.setTotalDurationMs(maxTotalDurationNs.get() / 1_000_000.0);
        processingStats.setThroughput(totalMessagesCount / (maxTotalDurationNs.get() / 1_000_000_000.0));

        processingStats.setEndToEndMeanLatency(endToEndStats.getMean());
        processingStats.setEndToEndP90Latency(endToEndStats.getPercentile(90));

        processingStats.setReqTransportMeanLatency(requestStats.getMean());
        processingStats.setReqTransportP90Latency(requestStats.getPercentile(90));

        processingStats.setRespTransportMeanLatency(responseStats.getMean());
        processingStats.setRespTransportP90Latency(responseStats.getPercentile(90));

        return processingStats;
    }
}
