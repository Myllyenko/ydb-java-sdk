package tech.ydb.coordination.rpc.grpc;

import java.util.concurrent.CompletableFuture;

import javax.annotation.PreDestroy;
import javax.annotation.WillClose;
import javax.annotation.WillNotClose;

import tech.ydb.coordination.rpc.CoordinationRpc;
import tech.ydb.core.Operations;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcReadWriteStream;
import tech.ydb.core.grpc.GrpcRequestSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.proto.coordination.AlterNodeRequest;
import tech.ydb.proto.coordination.AlterNodeResponse;
import tech.ydb.proto.coordination.CreateNodeRequest;
import tech.ydb.proto.coordination.CreateNodeResponse;
import tech.ydb.proto.coordination.DescribeNodeRequest;
import tech.ydb.proto.coordination.DescribeNodeResponse;
import tech.ydb.proto.coordination.DropNodeRequest;
import tech.ydb.proto.coordination.DropNodeResponse;
import tech.ydb.proto.coordination.SessionRequest;
import tech.ydb.proto.coordination.SessionResponse;
import tech.ydb.proto.coordination.v1.CoordinationServiceGrpc;

/**
 * @author Kirill Kurdyukov
 */
public class GrpcCoordinationRpc implements CoordinationRpc {

    private final GrpcTransport grpcTransport;
    private final boolean transportOwned;

    private GrpcCoordinationRpc(GrpcTransport grpcTransport, boolean transportOwned) {
        this.grpcTransport = grpcTransport;
        this.transportOwned = transportOwned;
    }

    public static GrpcCoordinationRpc useTransport(@WillNotClose GrpcTransport transport) {
        return new GrpcCoordinationRpc(transport, false);
    }

    public static GrpcCoordinationRpc ownTransport(@WillClose GrpcTransport transport) {
        return new GrpcCoordinationRpc(transport, true);
    }

    @Override
    public GrpcReadWriteStream<SessionResponse, SessionRequest> session() {
        return grpcTransport.readWriteStreamCall(
                CoordinationServiceGrpc.getSessionMethod(),
                GrpcRequestSettings.newBuilder().build()
        );
    }

    @Override
    public CompletableFuture<Status> createNode(CreateNodeRequest request, GrpcRequestSettings settings) {
        return grpcTransport.unaryCall(
                CoordinationServiceGrpc.getCreateNodeMethod(),
                settings,
                request
        ).thenApply(Operations.statusUnwrapper(CreateNodeResponse::getOperation));
    }

    @Override
    public CompletableFuture<Status> alterNode(AlterNodeRequest request, GrpcRequestSettings settings) {
        return grpcTransport.unaryCall(
                CoordinationServiceGrpc.getAlterNodeMethod(),
                settings,
                request
        ).thenApply(Operations.statusUnwrapper(AlterNodeResponse::getOperation));
    }

    @Override
    public CompletableFuture<Status> dropNode(DropNodeRequest request, GrpcRequestSettings settings) {
        return grpcTransport.unaryCall(
                CoordinationServiceGrpc.getDropNodeMethod(),
                settings,
                request
        ).thenApply(Operations.statusUnwrapper(DropNodeResponse::getOperation));
    }

    @Override
    public CompletableFuture<Status> describeNode(DescribeNodeRequest request, GrpcRequestSettings settings) {
        return grpcTransport.unaryCall(
                CoordinationServiceGrpc.getDescribeNodeMethod(),
                settings,
                request
        ).thenApply(Operations.statusUnwrapper(DescribeNodeResponse::getOperation));
    }

    @Override
    public String getDatabase() {
        return grpcTransport.getDatabase();
    }

    @PreDestroy
    public void close() {
        if (transportOwned) {
            grpcTransport.close();
        }
    }
}