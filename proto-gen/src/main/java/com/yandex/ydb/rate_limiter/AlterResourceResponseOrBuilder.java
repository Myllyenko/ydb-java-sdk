// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: kikimr/public/api/protos/ydb_rate_limiter.proto

package tech.ydb.rate_limiter;

public interface AlterResourceResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:Ydb.RateLimiter.AlterResourceResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * Holds AlterResourceResult in case of successful call.
   * </pre>
   *
   * <code>.Ydb.Operations.Operation operation = 1;</code>
   */
  boolean hasOperation();
  /**
   * <pre>
   * Holds AlterResourceResult in case of successful call.
   * </pre>
   *
   * <code>.Ydb.Operations.Operation operation = 1;</code>
   */
  tech.ydb.OperationProtos.Operation getOperation();
  /**
   * <pre>
   * Holds AlterResourceResult in case of successful call.
   * </pre>
   *
   * <code>.Ydb.Operations.Operation operation = 1;</code>
   */
  tech.ydb.OperationProtos.OperationOrBuilder getOperationOrBuilder();
}
