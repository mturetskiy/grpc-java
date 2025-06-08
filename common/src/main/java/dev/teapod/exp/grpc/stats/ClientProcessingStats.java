package dev.teapod.exp.grpc.stats;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class ClientProcessingStats {
    private List<Long> endToEndDurations;
    private List<Long> requestTransportDurations;
    private List<Long> responseTransportDurations;
    private Duration totalDuration; // for all messages

    public ClientProcessingStats(int count) {
        endToEndDurations = new ArrayList<>(count);
        requestTransportDurations = new ArrayList<>(count);
        responseTransportDurations = new ArrayList<>(count);
    }

    public void addStats(SingleMessageProcessingStats singleMessageProcessingStats) {
        this.endToEndDurations.add(singleMessageProcessingStats.getEndToEndDuration().toNanos());
        this.requestTransportDurations.add(singleMessageProcessingStats.getRequestTransportDuration().toNanos());
        this.responseTransportDurations.add(singleMessageProcessingStats.getResponsesTransportDuration().toNanos());
    }

    public ClientProcessingStats setTotalDuration(Duration totalDuration) {
        this.totalDuration = totalDuration;
        return this;
    }
}
