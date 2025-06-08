package dev.teapod.exp.grpc;

import dev.teapod.exp.grpc.stats.ClientProcessingStats;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class GrpcClientUtils {
    public static List<ClientProcessingStats> runClients(int clientsCount, FunctionEx<Integer, ClientProcessingStats> codeToRun) {
        List<ClientProcessingStats> result = new ArrayList<>(clientsCount);
        ExecutorService executorService = Executors.newFixedThreadPool(clientsCount);
        try {
            List<Future<ClientProcessingStats>> futures = new ArrayList<>(clientsCount);

            for (int i = 1; i <= clientsCount; i++) {
                int clientIndex = i;
                Future<ClientProcessingStats> future = executorService.submit(() -> codeToRun.apply(clientIndex));
                futures.add(future);
            }

            futures.forEach(f -> {
                try {
                    ClientProcessingStats processingStats = f.get(1, TimeUnit.HOURS);
                    if (processingStats == null) {
                        log.error("No stats available for one of the clients.");
                        return;
                    }

                    result.add(processingStats);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    log.error("Error during one of the clients processing", e);
                } catch (TimeoutException e) {
                    log.error("Timeout during one of the clients processing", e);
                }
            });

            return result;
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.MINUTES)) {
                    executorService.shutdownNow(); // Cancel currently executing tasks
                    if (!executorService.awaitTermination(60, TimeUnit.MINUTES))
                        log.error("Pool did not terminate.");
                }
            } catch (InterruptedException ex) {
                executorService.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }
}
