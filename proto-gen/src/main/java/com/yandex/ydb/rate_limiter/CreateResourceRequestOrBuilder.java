// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: kikimr/public/api/protos/ydb_rate_limiter.proto

package tech.ydb.rate_limiter;

public interface CreateResourceRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:Ydb.RateLimiter.CreateResourceRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>.Ydb.Operations.OperationParams operation_params = 1;</code>
   */
  boolean hasOperationParams();
  /**
   * <code>.Ydb.Operations.OperationParams operation_params = 1;</code>
   */
  tech.ydb.OperationProtos.OperationParams getOperationParams();
  /**
   * <code>.Ydb.Operations.OperationParams operation_params = 1;</code>
   */
  tech.ydb.OperationProtos.OperationParamsOrBuilder getOperationParamsOrBuilder();

  /**
   * <pre>
   * Path of a coordination node.
   * </pre>
   *
   * <code>string coordination_node_path = 2;</code>
   */
  java.lang.String getCoordinationNodePath();
  /**
   * <pre>
   * Path of a coordination node.
   * </pre>
   *
   * <code>string coordination_node_path = 2;</code>
   */
  com.google.protobuf.ByteString
      getCoordinationNodePathBytes();

  /**
   * <pre>
   * Resource properties.
   * </pre>
   *
   * <code>.Ydb.RateLimiter.Resource resource = 3;</code>
   */
  boolean hasResource();
  /**
   * <pre>
   * Resource properties.
   * </pre>
   *
   * <code>.Ydb.RateLimiter.Resource resource = 3;</code>
   */
  tech.ydb.rate_limiter.Resource getResource();
  /**
   * <pre>
   * Resource properties.
   * </pre>
   *
   * <code>.Ydb.RateLimiter.Resource resource = 3;</code>
   */
  tech.ydb.rate_limiter.ResourceOrBuilder getResourceOrBuilder();
}
