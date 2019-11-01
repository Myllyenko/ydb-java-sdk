// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: kikimr/public/api/protos/ydb_rate_limiter.proto

package tech.ydb.rate_limiter;

/**
 * Protobuf type {@code Ydb.RateLimiter.DescribeResourceResponse}
 */
public  final class DescribeResourceResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:Ydb.RateLimiter.DescribeResourceResponse)
    DescribeResourceResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use DescribeResourceResponse.newBuilder() to construct.
  private DescribeResourceResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private DescribeResourceResponse() {
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private DescribeResourceResponse(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!parseUnknownFieldProto3(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
          case 10: {
            tech.ydb.OperationProtos.Operation.Builder subBuilder = null;
            if (operation_ != null) {
              subBuilder = operation_.toBuilder();
            }
            operation_ = input.readMessage(tech.ydb.OperationProtos.Operation.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(operation_);
              operation_ = subBuilder.buildPartial();
            }

            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return tech.ydb.rate_limiter.RateLimiterProtos.internal_static_Ydb_RateLimiter_DescribeResourceResponse_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return tech.ydb.rate_limiter.RateLimiterProtos.internal_static_Ydb_RateLimiter_DescribeResourceResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            tech.ydb.rate_limiter.DescribeResourceResponse.class, tech.ydb.rate_limiter.DescribeResourceResponse.Builder.class);
  }

  public static final int OPERATION_FIELD_NUMBER = 1;
  private tech.ydb.OperationProtos.Operation operation_;
  /**
   * <pre>
   * Holds DescribeResourceResult in case of successful call.
   * </pre>
   *
   * <code>.Ydb.Operations.Operation operation = 1;</code>
   */
  public boolean hasOperation() {
    return operation_ != null;
  }
  /**
   * <pre>
   * Holds DescribeResourceResult in case of successful call.
   * </pre>
   *
   * <code>.Ydb.Operations.Operation operation = 1;</code>
   */
  public tech.ydb.OperationProtos.Operation getOperation() {
    return operation_ == null ? tech.ydb.OperationProtos.Operation.getDefaultInstance() : operation_;
  }
  /**
   * <pre>
   * Holds DescribeResourceResult in case of successful call.
   * </pre>
   *
   * <code>.Ydb.Operations.Operation operation = 1;</code>
   */
  public tech.ydb.OperationProtos.OperationOrBuilder getOperationOrBuilder() {
    return getOperation();
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (operation_ != null) {
      output.writeMessage(1, getOperation());
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (operation_ != null) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, getOperation());
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof tech.ydb.rate_limiter.DescribeResourceResponse)) {
      return super.equals(obj);
    }
    tech.ydb.rate_limiter.DescribeResourceResponse other = (tech.ydb.rate_limiter.DescribeResourceResponse) obj;

    boolean result = true;
    result = result && (hasOperation() == other.hasOperation());
    if (hasOperation()) {
      result = result && getOperation()
          .equals(other.getOperation());
    }
    result = result && unknownFields.equals(other.unknownFields);
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasOperation()) {
      hash = (37 * hash) + OPERATION_FIELD_NUMBER;
      hash = (53 * hash) + getOperation().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static tech.ydb.rate_limiter.DescribeResourceResponse parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(tech.ydb.rate_limiter.DescribeResourceResponse prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code Ydb.RateLimiter.DescribeResourceResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:Ydb.RateLimiter.DescribeResourceResponse)
      tech.ydb.rate_limiter.DescribeResourceResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return tech.ydb.rate_limiter.RateLimiterProtos.internal_static_Ydb_RateLimiter_DescribeResourceResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return tech.ydb.rate_limiter.RateLimiterProtos.internal_static_Ydb_RateLimiter_DescribeResourceResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              tech.ydb.rate_limiter.DescribeResourceResponse.class, tech.ydb.rate_limiter.DescribeResourceResponse.Builder.class);
    }

    // Construct using tech.ydb.rate_limiter.DescribeResourceResponse.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    public Builder clear() {
      super.clear();
      if (operationBuilder_ == null) {
        operation_ = null;
      } else {
        operation_ = null;
        operationBuilder_ = null;
      }
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return tech.ydb.rate_limiter.RateLimiterProtos.internal_static_Ydb_RateLimiter_DescribeResourceResponse_descriptor;
    }

    public tech.ydb.rate_limiter.DescribeResourceResponse getDefaultInstanceForType() {
      return tech.ydb.rate_limiter.DescribeResourceResponse.getDefaultInstance();
    }

    public tech.ydb.rate_limiter.DescribeResourceResponse build() {
      tech.ydb.rate_limiter.DescribeResourceResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public tech.ydb.rate_limiter.DescribeResourceResponse buildPartial() {
      tech.ydb.rate_limiter.DescribeResourceResponse result = new tech.ydb.rate_limiter.DescribeResourceResponse(this);
      if (operationBuilder_ == null) {
        result.operation_ = operation_;
      } else {
        result.operation_ = operationBuilder_.build();
      }
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof tech.ydb.rate_limiter.DescribeResourceResponse) {
        return mergeFrom((tech.ydb.rate_limiter.DescribeResourceResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(tech.ydb.rate_limiter.DescribeResourceResponse other) {
      if (other == tech.ydb.rate_limiter.DescribeResourceResponse.getDefaultInstance()) return this;
      if (other.hasOperation()) {
        mergeOperation(other.getOperation());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      tech.ydb.rate_limiter.DescribeResourceResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (tech.ydb.rate_limiter.DescribeResourceResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private tech.ydb.OperationProtos.Operation operation_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        tech.ydb.OperationProtos.Operation, tech.ydb.OperationProtos.Operation.Builder, tech.ydb.OperationProtos.OperationOrBuilder> operationBuilder_;
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    public boolean hasOperation() {
      return operationBuilder_ != null || operation_ != null;
    }
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    public tech.ydb.OperationProtos.Operation getOperation() {
      if (operationBuilder_ == null) {
        return operation_ == null ? tech.ydb.OperationProtos.Operation.getDefaultInstance() : operation_;
      } else {
        return operationBuilder_.getMessage();
      }
    }
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    public Builder setOperation(tech.ydb.OperationProtos.Operation value) {
      if (operationBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        operation_ = value;
        onChanged();
      } else {
        operationBuilder_.setMessage(value);
      }

      return this;
    }
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    public Builder setOperation(
        tech.ydb.OperationProtos.Operation.Builder builderForValue) {
      if (operationBuilder_ == null) {
        operation_ = builderForValue.build();
        onChanged();
      } else {
        operationBuilder_.setMessage(builderForValue.build());
      }

      return this;
    }
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    public Builder mergeOperation(tech.ydb.OperationProtos.Operation value) {
      if (operationBuilder_ == null) {
        if (operation_ != null) {
          operation_ =
            tech.ydb.OperationProtos.Operation.newBuilder(operation_).mergeFrom(value).buildPartial();
        } else {
          operation_ = value;
        }
        onChanged();
      } else {
        operationBuilder_.mergeFrom(value);
      }

      return this;
    }
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    public Builder clearOperation() {
      if (operationBuilder_ == null) {
        operation_ = null;
        onChanged();
      } else {
        operation_ = null;
        operationBuilder_ = null;
      }

      return this;
    }
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    public tech.ydb.OperationProtos.Operation.Builder getOperationBuilder() {
      
      onChanged();
      return getOperationFieldBuilder().getBuilder();
    }
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    public tech.ydb.OperationProtos.OperationOrBuilder getOperationOrBuilder() {
      if (operationBuilder_ != null) {
        return operationBuilder_.getMessageOrBuilder();
      } else {
        return operation_ == null ?
            tech.ydb.OperationProtos.Operation.getDefaultInstance() : operation_;
      }
    }
    /**
     * <pre>
     * Holds DescribeResourceResult in case of successful call.
     * </pre>
     *
     * <code>.Ydb.Operations.Operation operation = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        tech.ydb.OperationProtos.Operation, tech.ydb.OperationProtos.Operation.Builder, tech.ydb.OperationProtos.OperationOrBuilder> 
        getOperationFieldBuilder() {
      if (operationBuilder_ == null) {
        operationBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            tech.ydb.OperationProtos.Operation, tech.ydb.OperationProtos.Operation.Builder, tech.ydb.OperationProtos.OperationOrBuilder>(
                getOperation(),
                getParentForChildren(),
                isClean());
        operation_ = null;
      }
      return operationBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFieldsProto3(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:Ydb.RateLimiter.DescribeResourceResponse)
  }

  // @@protoc_insertion_point(class_scope:Ydb.RateLimiter.DescribeResourceResponse)
  private static final tech.ydb.rate_limiter.DescribeResourceResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new tech.ydb.rate_limiter.DescribeResourceResponse();
  }

  public static tech.ydb.rate_limiter.DescribeResourceResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<DescribeResourceResponse>
      PARSER = new com.google.protobuf.AbstractParser<DescribeResourceResponse>() {
    public DescribeResourceResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new DescribeResourceResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<DescribeResourceResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<DescribeResourceResponse> getParserForType() {
    return PARSER;
  }

  public tech.ydb.rate_limiter.DescribeResourceResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

