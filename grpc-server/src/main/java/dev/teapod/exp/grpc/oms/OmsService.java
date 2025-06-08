package dev.teapod.exp.grpc.oms;

import dev.teapod.exp.grpc.oms.OmsOuterClass.NewOrderRequest;
import dev.teapod.exp.grpc.oms.OmsOuterClass.NewOrderResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static dev.teapod.exp.grpc.GrpcConverterUtils.*;

@Slf4j
public class OmsService extends OmsGrpc.OmsImplBase {
    private final AtomicLong orderIdGen = new AtomicLong(System.currentTimeMillis());
    private final BigDecimal feeRate = BigDecimal.valueOf(0.12);

    @Override
    public void sendOrder(NewOrderRequest request, StreamObserver<NewOrderResponse> responseObserver) {
        NewOrderResponse response = processOrder(request);

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        log.debug("Done with new order request processing. RequestId: {}, clientName:{}, assigned orderId: {}",
                request.getRequestId(), request.getClientName(), response.getOrderId());
    }

    @Override
    public StreamObserver<NewOrderRequest> sendOrdersAsync(StreamObserver<NewOrderResponse> responseObserver) {
        return new StreamObserver<>() {

            @Override
            public void onNext(NewOrderRequest request) {
                NewOrderResponse response = processOrder(request);
                responseObserver.onNext(response);

                log.debug("Done with new streaming order request processing. RequestId: {}, clientName:{}, assigned orderId: {}",
                        request.getRequestId(), request.getClientName(), response.getOrderId());
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("On error during streaming order processing.", throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
                log.info("On completed.");
            }
        };
    }

    private NewOrderResponse processOrder(NewOrderRequest request) {
        Instant receivedTs = Instant.now();
        BigDecimal qty = fromDecimalValue(request.getQuantity());

        log.debug("Received new order request. requestId: {}, UUID: {}, clOrderId: {}, sourceSystem: {}, qty: {}",
                request.getRequestId(), request.getUuid(), request.getClOrderId(), request.getSourceSystem(), qty);

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

        return orderResponse.toBuilder()
                .setProcessedTimestampUtc(fromInstant(Instant.now()))
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