package jrpc;

@FunctionalInterface
public interface GrpcHandler {
    MessageObject apply(MessageObject messageObject) throws Exception;
}