package dev.teapod.exp.grpc.echo;

import io.grpc.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class EchoClient {
    private final AtomicInteger idsGen = new AtomicInteger();
    private final EchoGrpc.EchoBlockingStub blockingStub;

    public EchoClient(ManagedChannel channel, String target) {
        log.info("Created client with target: {}", target);
        this.blockingStub = EchoGrpc.newBlockingStub(channel);
    }

    public void sendEchoRequest(String message) {
        EchoOuterClass.EchoRequest request = EchoOuterClass.EchoRequest.newBuilder()
                .setRequestId(idsGen.getAndIncrement())
                .setMessage(message)
                .build();

        try {
            log.info("Sent request: {}", request);
            EchoOuterClass.EchoResponse echoResponse = this.blockingStub.sendEcho(request);

            log.info("Got response: {}", echoResponse);
        } catch (StatusRuntimeException e) {
            log.error("Error during grpc call", e);
        }
    }

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 8976;
        String target = host + ":" + port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();
        try {
            EchoClient echoClient = new EchoClient(channel, target);

            for (int i = 1; i <= 10; i++) {
                echoClient.sendEchoRequest("Echo #" + i);
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            log.error("Error during client operations", e);
        } finally {
            channel.shutdownNow().awaitTermination(60, TimeUnit.SECONDS);
        }
    }
}