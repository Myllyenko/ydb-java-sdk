// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: kikimr/public/api/protos/ydb_rate_limiter.proto

package tech.ydb.rate_limiter;

public interface ListResourcesResultOrBuilder extends
    // @@protoc_insertion_point(interface_extends:Ydb.RateLimiter.ListResourcesResult)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated string resource_paths = 1;</code>
   */
  java.util.List<java.lang.String>
      getResourcePathsList();
  /**
   * <code>repeated string resource_paths = 1;</code>
   */
  int getResourcePathsCount();
  /**
   * <code>repeated string resource_paths = 1;</code>
   */
  java.lang.String getResourcePaths(int index);
  /**
   * <code>repeated string resource_paths = 1;</code>
   */
  com.google.protobuf.ByteString
      getResourcePathsBytes(int index);
}
