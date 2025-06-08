package dev.teapod.exp.grpc.oms;

import dev.teapod.exp.grpc.GrpcClientUtils;
import dev.teapod.exp.grpc.stats.ClientProcessingStats;
import dev.teapod.exp.grpc.stats.ProcessingStats;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static dev.teapod.exp.grpc.GrpcConverterUtils.fromDecimalValue;
import static dev.teapod.exp.grpc.GrpcConverterUtils.fromTimestamp;
import static dev.teapod.exp.grpc.stats.StatsUtils.collectProcessingStats;
import static java.util.concurrent.TimeUnit.*;

@Slf4j
public class OmsAsyncClient extends OmsClient {
    private final OmsGrpc.OmsStub asyncStub;

    public OmsAsyncClient(ManagedChannel channel, String target) {
        log.info("Created async oms client with target: {}", target);
        this.asyncStub = OmsGrpc.newStub(channel);
    }

    public ClientProcessingStats sendOrdersAsync(String clientName, int clientId, int ordersCount, int noAllocsCount) {
        log.info("[{}] Sending stream of {} messages to the server.", clientName, ordersCount);

        CountDownLatch latch = new CountDownLatch(1);
        ClientProcessingStats stats = new ClientProcessingStats(ordersCount);
        Map<Long, Instant> requestSentTimes = new ConcurrentHashMap<>(ordersCount, 1.0f);

        Instant start = Instant.now();

        StreamObserver<OmsOuterClass.NewOrderResponse> responseStreamObserver = new StreamObserver<>() {

            @Override
            public void onNext(OmsOuterClass.NewOrderResponse newOrderResponse) {
                BigDecimal grossAmt = fromDecimalValue(newOrderResponse.getGrossAmt());
                BigDecimal netAmt = fromDecimalValue(newOrderResponse.getNetAmt());
                BigDecimal feesAmt = fromDecimalValue(newOrderResponse.getFeesAmt());

                Instant responseReceivedTime = Instant.now();
                Instant requestSentTime = requestSentTimes.remove(newOrderResponse.getRequestId());
                Instant requestReceivedTime = fromTimestamp(newOrderResponse.getReceivedTimestampUtc());
                Instant responseSentTime = fromTimestamp(newOrderResponse.getProcessedTimestampUtc());

                Duration requestTransportDuration = Duration.between(requestSentTime, requestReceivedTime);
                Duration responseTransportDuration = Duration.between(responseSentTime, responseReceivedTime);
                Duration endToEndDuration = Duration.between(requestSentTime, responseReceivedTime);

                stats.getRequestTransportDurations().add(requestTransportDuration.toNanos());
                stats.getResponseTransportDurations().add(responseTransportDuration.toNanos());
                stats.getEndToEndDurations().add(endToEndDuration.toNanos());

                log.debug("[{}] Received response for order, requestId: {}, endToEndTime: {} micros. grossAmt: {}, netAmt: {}, feesAmt: {}",
                        clientName, newOrderResponse.getRequestId(), MICROSECONDS.convert(endToEndDuration), grossAmt, netAmt, feesAmt);
            }

            @Override
            public void onError(Throwable e) {
                log.error("[{}] Error during order processing at server side.", clientName, e);
            }

            @Override
            public void onCompleted() {
                latch.countDown();
                log.info("[{}] Done with all orders.", clientName);
            }
        };
        try {
            StreamObserver<OmsOuterClass.NewOrderRequest> requestStreamObserver = asyncStub.sendOrdersAsync(responseStreamObserver);

            for (int i = 0; i < ordersCount; i++) {
                OmsOuterClass.NewOrderRequest newOrderRequest = createOrderRequest(clientName, clientId, i, noAllocsCount);
                requestSentTimes.put(newOrderRequest.getRequestId(), Instant.now());
                requestStreamObserver.onNext(newOrderRequest);
                log.debug("[{}] Sent order requestOd: {}", clientName, newOrderRequest.getRequestId());
            }

            requestStreamObserver.onCompleted();

            latch.await(1, HOURS);

            Duration totalDuration = Duration.between(start, Instant.now());

            log.info("[{}] Done with streaming of {} messages. Total duration: {} ms.",
                    clientName, ordersCount, MILLISECONDS.convert(totalDuration));

            return stats.setTotalDuration(totalDuration);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (StatusRuntimeException e) {
            log.error("[{}] Error during streaming for new orders.", clientName, e);
        }

        return null;
    }

    private static ClientProcessingStats runAsyncClient(int clientId, String host, int port, int messagesCount, int noAllocsCount) throws InterruptedException {
        String target = host + ":" + port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        try {
            OmsAsyncClient omsClient = new OmsAsyncClient(channel, target);
            String clientName = "OmsAsyncClient-" + clientId;

            return omsClient.sendOrdersAsync(clientName, clientId, messagesCount, noAllocsCount);
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
                clientIdx -> runAsyncClient(clientIdx, host, port, messagesCount, noAllocsCount));

        ProcessingStats stats = collectProcessingStats(processingStats, totalMessagesCount);
        stats.printStats();

        log.info("Done with {} clients. Shutting down.", clientsCount);
    }
}
