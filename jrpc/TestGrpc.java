package jrpc;

public class TestGrpc {
    public static void main(String[] args) throws Exception {
        new GrpcServer("test.proto").start();
    }
}
