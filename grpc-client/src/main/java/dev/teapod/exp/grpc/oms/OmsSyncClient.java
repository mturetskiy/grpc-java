package dev.teapod.exp.grpc.oms;

import dev.teapod.exp.grpc.GrpcClientUtils;
import dev.teapod.exp.grpc.stats.ClientProcessingStats;
import dev.teapod.exp.grpc.stats.ProcessingStats;
import dev.teapod.exp.grpc.stats.SingleMessageProcessingStats;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static dev.teapod.exp.grpc.GrpcConverterUtils.fromDecimalValue;
import static dev.teapod.exp.grpc.GrpcConverterUtils.fromTimestamp;
import static dev.teapod.exp.grpc.stats.StatsUtils.collectProcessingStats;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
public class OmsSyncClient extends OmsClient {
    protected final OmsGrpc.OmsBlockingV2Stub blockingStub;

    public OmsSyncClient(ManagedChannel channel, String target) {
        log.info("Created sync oms client with target: {}", target);
        this.blockingStub = OmsGrpc.newBlockingV2Stub(channel);
    }

    public SingleMessageProcessingStats sendOrderSync(String clientName, int clientId, int orderIndex, int noAllocsCount) {
        OmsOuterClass.NewOrderRequest newOrderRequest = createOrderRequest(clientName, clientId, orderIndex, noAllocsCount);
        try {
            Instant requestSentTime = Instant.now();
            OmsOuterClass.NewOrderResponse response = this.blockingStub.sendOrder(newOrderRequest);

            BigDecimal grossAmt = fromDecimalValue(response.getGrossAmt());
            BigDecimal netAmt = fromDecimalValue(response.getNetAmt());
            BigDecimal feesAmt = fromDecimalValue(response.getFeesAmt());

            Instant responseReceivedTime = Instant.now();
            Instant requestReceivedTime = fromTimestamp(response.getReceivedTimestampUtc());
            Instant responseSentTime = fromTimestamp(response.getProcessedTimestampUtc());

            Duration requestTransportDuration = Duration.between(requestSentTime, requestReceivedTime);
            Duration responseTransportDuration = Duration.between(responseSentTime, responseReceivedTime);
            Duration endToEndDuration = Duration.between(requestSentTime, responseReceivedTime);

            log.debug("[{}] Received response for order, requestId: {}, endToEndTime: {} micros. grossAmt: {}, netAmt: {}, feesAmt: {}",
                    clientName, response.getRequestId(), MICROSECONDS.convert(endToEndDuration), grossAmt, netAmt, feesAmt);

            return new SingleMessageProcessingStats(endToEndDuration, requestTransportDuration, responseTransportDuration);
        } catch (StatusRuntimeException e) {
            log.error("[{}] Error during grpc call for order# {}", clientName, orderIndex, e);
            return null;
        }
    }

    private static ClientProcessingStats runClient(int clientId, String host, int port, int messagesCount, int noAllocsCount) throws InterruptedException {
        String target = host + ":" + port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        try {
            OmsSyncClient omsClient = new OmsSyncClient(channel, target);
            String clientName = "OmsSyncClient-" + clientId;
            log.info("[{}] Sending {} messages to the server", clientName, messagesCount);

            ClientProcessingStats processingStats = new ClientProcessingStats(messagesCount);

            Instant start = Instant.now();
            for (int i = 0; i < messagesCount; i++) {
                SingleMessageProcessingStats singleMsgStats = omsClient.sendOrderSync(clientName, clientId, i, noAllocsCount);
                processingStats.addStats(singleMsgStats);
            }

            Duration totalDuration = Duration.between(start, Instant.now());
            log.info("[{}] Done with {} messages. Total duration: {} ms, Stopping the client.",
                    clientName, messagesCount, MILLISECONDS.convert(totalDuration));
            return processingStats.setTotalDuration(totalDuration);
        } catch (Exception e) {
            log.error("Error during client operations", e);
            return null;
        } finally {
            channel.shutdownNow().awaitTermination(60, TimeUnit.SECONDS);
        }
    }

    public static void main(String[] args) {
        String host = "127.0.0.1";
//        String host = "mtair.local";
        int port = 8977;
        int clientsCount = 10;
        int messagesCount = 100_000;
        int noAllocsCount = 5;

        int totalMessagesCount = clientsCount * messagesCount;

        List<ClientProcessingStats> processingStats = GrpcClientUtils.runClients(clientsCount,
                clientIdx -> runClient(clientIdx, host, port, messagesCount, noAllocsCount));

        ProcessingStats stats = collectProcessingStats(processingStats, totalMessagesCount);
        stats.printStats();

        log.info("Done with {} clients. Shutting down.", clientsCount);
    }
}
