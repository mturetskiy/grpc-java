package dev.teapod.exp.grpc.echo;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EchoService extends EchoGrpc.EchoImplBase {
    @Override
    public void sendEcho(EchoOuterClass.EchoRequest request, StreamObserver<EchoOuterClass.EchoResponse> responseObserver) {
        log.info("received echo request: {}", request.getMessage());

        EchoOuterClass.EchoResponse response = EchoOuterClass.EchoResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setMessage("Response to msg: " + request.getMessage())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}