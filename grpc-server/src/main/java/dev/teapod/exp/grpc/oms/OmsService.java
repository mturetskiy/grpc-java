package dev.teapod.exp.grpc.oms;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import dev.teapod.exp.grpc.oms.OmsOuterClass.NewOrderRequest;
import dev.teapod.exp.grpc.oms.OmsOuterClass.NewOrderResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class OmsService extends OmsGrpc.OmsImplBase {
    private final AtomicLong orderIdGen = new AtomicLong(System.currentTimeMillis());
    private final BigDecimal feeRate = BigDecimal.valueOf(0.12);

    @Override
    public void sendOrder(NewOrderRequest request, StreamObserver<NewOrderResponse> responseObserver) {
        Instant receivedTs = Instant.now();
        BigDecimal qty = fromDecimalValue(request.getQuantity());
//
//        log.info("Received new order request. requestId: {}, UUID: {}, clOrderId: {}, sourceSystem: {}, qty: {}",
//                request.getRequestId(), request.getUuid(), request.getClOrderId(), request.getSourceSystem(), qty);

        String newOrderId = "OMS_ORDER_ID-" + orderIdGen.getAndIncrement();
        BigDecimal netAmt = calculateNetAmt(request);
        BigDecimal feesAmt = netAmt.multiply(feeRate);
        BigDecimal grossAmt = netAmt.add(feesAmt);

        NewOrderResponse orderResponse = NewOrderResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setUuid(request.getUuid())
                .setClOrderId(request.getClOrderId())
                .setOrderId(newOrderId)
                .setOrderStatus(OmsOuterClass.OrderStatus.Accepted)
                .setReceivedTimestampUtc(fromInstant(receivedTs))
                .setGrossAmt(fromBigDecimal(grossAmt))
                .setNetAmt(fromBigDecimal(netAmt))
                .setFeesAmt(fromBigDecimal(feesAmt))
                .build();

        NewOrderResponse response = orderResponse.toBuilder()
                .setProcessedTimestampUtc(fromInstant(Instant.now()))
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        log.info("Done with new order request processing. RequestId: {}, clientName:{}, assigned orderId: {}",
                request.getRequestId(), request.getClientName(), newOrderId);
    }

    private Timestamp fromInstant(Instant ts) {
        return Timestamp.newBuilder()
                .setSeconds(ts.getEpochSecond())
                .setNanos(ts.getNano())
                .build();
    }

    private BigDecimal fromDecimalValue(OmsOuterClass.DecimalValue dv) {
        BigInteger bigInteger = new BigInteger(dv.getValue().toByteArray());
        MathContext mc = new MathContext(dv.getPrecision());
        return new BigDecimal(bigInteger, dv.getScale(), mc);
    }

    private OmsOuterClass.DecimalValue fromBigDecimal(BigDecimal bd) {
        return OmsOuterClass.DecimalValue.newBuilder()
                .setScale(bd.scale())
                .setPrecision(bd.precision())
                .setValue(ByteString.copyFrom(bd.unscaledValue().toByteArray()))
                .build();
    }

    private BigDecimal calculateNetAmt(NewOrderRequest request) {
        return request.getNoAllocsList().stream().map(noAlloc -> {
            BigDecimal price = fromDecimalValue(noAlloc.getAllocPrice());
            BigDecimal qty = fromDecimalValue(noAlloc.getAllocQty());
            return price.multiply(qty);
        }).reduce(BigDecimal::add).orElse(BigDecimal.ZERO);
    }
}