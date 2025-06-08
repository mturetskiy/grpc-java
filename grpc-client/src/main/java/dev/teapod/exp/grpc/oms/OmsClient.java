package dev.teapod.exp.grpc.oms;

import dev.teapod.exp.grpc.oms.OmsOuterClass.NewOrderRequest;
import dev.teapod.exp.grpc.oms.OmsOuterClass.NoAlloc;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static dev.teapod.exp.grpc.GrpcConverterUtils.fromBigDecimal;
import static dev.teapod.exp.grpc.GrpcConverterUtils.fromInstant;

@Slf4j
public abstract class OmsClient {
    protected static final AtomicLong requestIdGen = new AtomicLong(1_000_000);
    protected static final AtomicLong clOrderIdGen = new AtomicLong(1);
    protected static final AtomicLong allocIdGen = new AtomicLong(1);
    protected static final List<Integer> accounts = List.of(12, 50, 150, 230, 1200, 1400, 1550, 2000, 2300, 5000);

    protected final Random rnd = new Random(System.currentTimeMillis());

    protected NewOrderRequest createOrderRequest(String clientName, int clientId, int orderIndex, int noAllocsCount) {
        NewOrderRequest.Builder builder = NewOrderRequest.newBuilder()
                .setRequestId(requestIdGen.incrementAndGet())
                .setUuid(UUID.randomUUID().toString())
                .setClOrderId(clientName + "_CL_ORDERID-" + clOrderIdGen.incrementAndGet())
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

        return builder.build();
    }

    private NoAlloc createNoAllocs(BigDecimal qty, String clientName, int allocIndex) {
        return NoAlloc.newBuilder()
                .setAllocAccount(accounts.get(rnd.nextInt(accounts.size())))
                .setAllocPrice(fromBigDecimal(BigDecimal.valueOf(12.5846362)))
                .setAllocQty(fromBigDecimal(qty))
                .setAllocSettlCurrency("USD")
                .setIndividualAllocId("CL_ALLOC_ID_" + clientName + "_IDX_" + allocIndex + "_ID-" + allocIdGen.incrementAndGet())
                .build();
    }


}