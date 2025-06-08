package dev.teapod.exp.grpc.stats;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class StatsUtils {
    public static ProcessingStats collectProcessingStats(List<ClientProcessingStats> clientStats, int totalMessagesCount) {
        ProcessingStats processingStats = new ProcessingStats();
        DescriptiveStatistics dStats = new DescriptiveStatistics(new double[totalMessagesCount]);

        AtomicLong maxTotalDurationNs = new AtomicLong(0);
        clientStats.forEach(cs -> {
            List<Long> durations = cs.getEndToEndDurations();
            durations.forEach(durationNs -> {
                dStats.addValue(Double.valueOf(durationNs));
            });

            long totalDurationNs = cs.getTotalDuration().toNanos();
            maxTotalDurationNs.updateAndGet(val -> Math.max(val, totalDurationNs));
        });

        processingStats.setTotalMessages(totalMessagesCount);
        processingStats.setTotalDurationMs(maxTotalDurationNs.get() / 1_000_000.0);
        processingStats.setMean(dStats.getMean());
        processingStats.setP90(dStats.getPercentile(90));
        processingStats.setThroughput(totalMessagesCount / (maxTotalDurationNs.get() / 1_000_000_000.0));

        return processingStats;
    }
}
