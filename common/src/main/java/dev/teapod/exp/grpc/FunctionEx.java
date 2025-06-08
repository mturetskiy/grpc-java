package dev.teapod.exp.grpc;

@FunctionalInterface
public interface FunctionEx<T, R> {
    R apply(T t) throws Exception;
}
