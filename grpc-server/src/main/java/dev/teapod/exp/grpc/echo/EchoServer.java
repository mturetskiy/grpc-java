package dev.teapod.exp.grpc.echo;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EchoServer {
    private final int port;
    private final Server grpcServer;

    public EchoServer(int port) {
        this.port = port;
        ServerBuilder<?> serverBuilder = Grpc.newServerBuilderForPort(this.port, InsecureServerCredentials.create());

        this.grpcServer = serverBuilder
                .addService(new EchoService())
                .build();
    }

    public void start() throws IOException {
        log.info("Starting the server at port: {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down the server.");

            try {
                EchoServer.this.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            log.info("Shut down successfully.");
        }));

        grpcServer.start();

        log.info("Server has been started.");
    }

    public void stop() throws InterruptedException {
        log.info("Stopping the server.");
        if (grpcServer != null) {
            grpcServer.shutdown().awaitTermination(60, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {
        EchoServer echoServer = new EchoServer(8976);
        echoServer.start();
        echoServer.blockUntilShutdown();
    }
}