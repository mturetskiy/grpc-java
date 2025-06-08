package dev.teapod.exp.grpc.oms;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import dev.teapod.exp.grpc.oms.OmsOuterClass.NewOrderRequest;
import dev.teapod.exp.grpc.oms.OmsOuterClass.NoAlloc;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
public class OmsClient {
    private final AtomicLong requestIdGen = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong clOrderIdGen = new AtomicLong(1);
    private final AtomicLong allocIdGen = new AtomicLong(1);
    private final List<Integer> accounts = List.of(12, 50, 150, 230, 1200, 1400, 1550, 2000, 2300, 5000);
    private final Random rnd = new Random(System.currentTimeMillis());

    private final OmsGrpc.OmsBlockingStub blockingStub;

    public OmsClient(ManagedChannel channel, String target) {
        log.info("Created oms client with target: {}", target);
        this.blockingStub = OmsGrpc.newBlockingStub(channel);
    }

    public long sendOrderMessage(String clientName, int clientId, int orderIndex, int noAllocsCount) {
        Instant startTime = Instant.now();
        NewOrderRequest.Builder builder = NewOrderRequest.newBuilder()
                .setRequestId(requestIdGen.getAndIncrement())
                .setUuid(UUID.randomUUID().toString())
                .setClOrderId(clientName + "_CL_ORDERID-" + clOrderIdGen.getAndIncrement())
                .setClientId(clientId)
                .setClientName(clientName)
                .setOriginatingSystem("GenericOmsClient")
                .setSourceSystem("OmsClient")
                .setCreatedTimestampUtc(fromInstant(Instant.now()))
                .setOrderType(OmsOuterClass.OrderType.Marker)
                .setSide(OmsOuterClass.Side.Buy)
                .setSecurityName("IBM.L")
                .setCurrency("USD")
                .setSettlCurrency("GBP")
                .setPrice(fromBigDecimal(BigDecimal.valueOf(12.56)))
                .setTradeDateTimestampUtc(fromInstant(Instant.now()))
                .setSettleDateTimestampUtc(fromInstant(Instant.now().plus(3, ChronoUnit.DAYS)))
                .setExpireTimestampUtc(fromInstant(Instant.now().plus(4, ChronoUnit.HOURS)))
                .setExecInst("Specific instructions #2")
                .setHandleInst("MANUAL_HANDLING")
                .setTraderName("TraderJoe")
                .setDescription("An ordinary order")
                .setDesignation("senior manager- accounts");

        BigDecimal totalOrderQty = BigDecimal.ZERO;
        for (int i = 0; i < noAllocsCount; i++) {
            BigDecimal allocQty = BigDecimal.valueOf(rnd.nextInt(1000));
            builder.addNoAllocs(createNoAllocs(allocQty, clientName, i));
            totalOrderQty = totalOrderQty.add(allocQty);
        }

        builder.setQuantity(fromBigDecimal(totalOrderQty));
        builder.addNotes("ORD#" + builder.getClOrderId() + "#" + builder.getRequestId() + "#" + builder.getSide().name());
        builder.addNotes("SRC#" + builder.getSourceSystem() + "#" + builder.getOriginatingSystem());
        builder.addNotes("TRD#" + builder.getTraderName() + "#" + builder.getClientName());
        builder.addNotes("SEC#" + builder.getSecurityName() + "#" + builder.getCurrency() + "#" + builder.getSettlCurrency());
        builder.addNotes("REQ_QTY#" + builder.getRequestId() + "#" + totalOrderQty);

        try {
            NewOrderRequest newOrderRequest = builder.build();
            Instant transportStart = Instant.now();
            OmsOuterClass.NewOrderResponse response = this.blockingStub.sendOrder(newOrderRequest);

            BigDecimal grossAmt = fromDecimalValue(response.getGrossAmt());
            BigDecimal netAmt = fromDecimalValue(response.getNetAmt());
            BigDecimal feesAmt = fromDecimalValue(response.getFeesAmt());

            Instant endTime = Instant.now();
            Duration endToEndTime = Duration.between(startTime, endTime);

//            Duration buildTime = Duration.between(startTime, transportStart);
//            Duration serverTime = Duration.between(fromTimestamp(response.getReceivedTimestampUtc()),
//                    fromTimestamp(response.getProcessedTimestampUtc()));
//            Duration transportTime = Duration.between(transportStart, Instant.now()).minus(serverTime);
//
//            log.info("[{}] Done with order# {}. RequestId: {}, clOrderId: {}, orderStatus: {}, " +
//                            "grossAmt: {}, netAmt: {}, feesAmt: {}, " +
//                            "Time build: {} micros, transport: {} micros, server: {} micros, endToEnd: {} micros",
//                    clientName, orderIndex, response.getRequestId(), response.getClOrderId(), response.getOrderStatus(),
//                    grossAmt, netAmt, feesAmt,
//                    MICROSECONDS.convert(buildTime), MICROSECONDS.convert(transportTime),
//                    MICROSECONDS.convert(serverTime), MICROSECONDS.convert(endToEndTime));

            return endToEndTime.toNanos();
        } catch (StatusRuntimeException e) {
            log.error("[{}] Error during grpc call for order# {}", clientName, orderIndex, e);
            return 0;
        }
    }

    private NoAlloc createNoAllocs(BigDecimal qty, String clientName, int allocIndex) {
        return NoAlloc.newBuilder()
                .setAllocAccount(accounts.get(rnd.nextInt(accounts.size())))
                .setAllocPrice(fromBigDecimal(BigDecimal.valueOf(12.5846362)))
                .setAllocQty(fromBigDecimal(qty))
                .setAllocSettlCurrency("USD")
                .setIndividualAllocId("CL_ALLOC_ID_" + clientName + "_IDX_" + allocIndex + "_ID-" + allocIdGen.getAndIncrement())
                .build();
    }

    private Timestamp fromInstant(Instant ts) {
        return Timestamp.newBuilder()
                .setSeconds(ts.getEpochSecond())
                .setNanos(ts.getNano())
                .build();
    }

    private Instant fromTimestamp(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private OmsOuterClass.DecimalValue fromBigDecimal(BigDecimal bd) {
        return OmsOuterClass.DecimalValue.newBuilder()
                .setScale(bd.scale())
                .setPrecision(bd.precision())
                .setValue(ByteString.copyFrom(bd.unscaledValue().toByteArray()))
                .build();
    }

    private BigDecimal fromDecimalValue(OmsOuterClass.DecimalValue dv) {
        BigInteger bigInteger = new BigInteger(dv.getValue().toByteArray());
        MathContext mc = new MathContext(dv.getPrecision());
        return new BigDecimal(bigInteger, dv.getScale(), mc);
    }

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 8977;
        int clientsCount = 100;
        int messagesCount = 10_000;
        int noAllocsCount = 5;

        try(ExecutorService executorService = Executors.newFixedThreadPool(clientsCount)) {
            List<Future<ProcessingStats>> futures = new ArrayList<>(clientsCount);
            int totalMessagesCount = clientsCount * messagesCount;
            DescriptiveStatistics stats = new DescriptiveStatistics(new double[totalMessagesCount]);

            for (int i = 1; i <= clientsCount; i++) {
                int clientIndex = i;
                Future<ProcessingStats> future = executorService.submit(() -> runClient(clientIndex, host, port, messagesCount, noAllocsCount));
                futures.add(future);
            }

            AtomicLong maxTotalDuration = new AtomicLong(0);
            futures.stream().forEach(f -> {
                try {
                    ProcessingStats processingStats = f.get();
                    if (processingStats == null) {
                        log.error("No stats available for one of the clients.");
                        return;
                    }

                    List<Long> durations = processingStats.getDurations();
                    durations.forEach(durationNs -> {
                        stats.addValue(Double.valueOf(durationNs));
                    });

                    long totalDurationNs = processingStats.getTotalDuration().toNanos();
                    maxTotalDuration.updateAndGet(val -> Math.max(val, totalDurationNs));

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    log.error("Error during one of the clients processing", e);
                }
            });

            double p90 = stats.getPercentile(90);
            double mean = stats.getMean();
            double totalDurationMs = maxTotalDuration.get() / 1_000_000.0;
            double throughput = totalMessagesCount / totalDurationMs * 1000.0;

            log.info("Result stats:\n\ttotal messages: {}\n\ttotalTime: {} ms" +
                            "\n\tmean latency: {} micros\n\tp90 latency: {} micros\n\tthroughput: {} msg/sec",
                    totalMessagesCount, (float) totalDurationMs,
                    (float) (mean / 1000.0), (float) (p90 / 1000.0), (float) throughput);
        }


        log.info("Done with {} clients. Shutting down.", clientsCount);
    }

    private static ProcessingStats runClient(int clientId, String host, int port, int messagesCount, int noAllocsCount) throws InterruptedException {
        String target = host + ":" + port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        try {
            OmsClient omsClient = new OmsClient(channel, target);
            String clientName = "OmsClient-" + clientId;
            log.info("[{}] Sending {} messages to the server", clientName, messagesCount);

            List<Long> durations = new ArrayList<>(messagesCount);

            Instant start = Instant.now();
            for (int i = 0; i < messagesCount; i++) {
                long durationNs = omsClient.sendOrderMessage(clientName, clientId, i, noAllocsCount);
                durations.add(durationNs);
            }
            Duration totalDuration = Duration.between(start, Instant.now());
            log.info("[{}] Done with {} messages. Total duration: {} ms, Stopping the client.",
                    clientName, messagesCount, MILLISECONDS.convert(totalDuration));
            return new ProcessingStats(durations, totalDuration);
        } catch (Exception e) {
            log.error("Error during client operations", e);
            return null;
        } finally {
            channel.shutdownNow().awaitTermination(60, TimeUnit.SECONDS);
        }
    }

    @Data
    @AllArgsConstructor
    static class ProcessingStats {
        private List<Long> durations;
        private Duration totalDuration;
    }
}