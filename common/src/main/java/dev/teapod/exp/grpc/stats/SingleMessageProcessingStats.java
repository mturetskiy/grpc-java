package dev.teapod.exp.grpc.stats;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Duration;

@Data
@AllArgsConstructor
public class SingleMessageProcessingStats {
    private Duration endToEndDuration;
    private Duration requestTransportDuration;
    private Duration responsesTransportDuration;
}
