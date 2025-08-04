package com.zh.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.63.0)",
    comments = "Source: crypto_service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class CryptoServiceGrpc {

  private CryptoServiceGrpc() {}

  public static final String SERVICE_NAME = "CryptoService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<CryptoProto.EncryptRequest,
      CryptoProto.EncryptResponse> getEncryptMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Encrypt",
      requestType = CryptoProto.EncryptRequest.class,
      responseType = CryptoProto.EncryptResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<CryptoProto.EncryptRequest,
      CryptoProto.EncryptResponse> getEncryptMethod() {
    io.grpc.MethodDescriptor<CryptoProto.EncryptRequest, CryptoProto.EncryptResponse> getEncryptMethod;
    if ((getEncryptMethod = CryptoServiceGrpc.getEncryptMethod) == null) {
      synchronized (CryptoServiceGrpc.class) {
        if ((getEncryptMethod = CryptoServiceGrpc.getEncryptMethod) == null) {
          CryptoServiceGrpc.getEncryptMethod = getEncryptMethod =
              io.grpc.MethodDescriptor.<CryptoProto.EncryptRequest, CryptoProto.EncryptResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Encrypt"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CryptoProto.EncryptRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CryptoProto.EncryptResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CryptoServiceMethodDescriptorSupplier("Encrypt"))
              .build();
        }
      }
    }
    return getEncryptMethod;
  }

  private static volatile io.grpc.MethodDescriptor<CryptoProto.DecryptRequest,
      CryptoProto.DecryptResponse> getDecryptMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Decrypt",
      requestType = CryptoProto.DecryptRequest.class,
      responseType = CryptoProto.DecryptResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<CryptoProto.DecryptRequest,
      CryptoProto.DecryptResponse> getDecryptMethod() {
    io.grpc.MethodDescriptor<CryptoProto.DecryptRequest, CryptoProto.DecryptResponse> getDecryptMethod;
    if ((getDecryptMethod = CryptoServiceGrpc.getDecryptMethod) == null) {
      synchronized (CryptoServiceGrpc.class) {
        if ((getDecryptMethod = CryptoServiceGrpc.getDecryptMethod) == null) {
          CryptoServiceGrpc.getDecryptMethod = getDecryptMethod =
              io.grpc.MethodDescriptor.<CryptoProto.DecryptRequest, CryptoProto.DecryptResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Decrypt"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CryptoProto.DecryptRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CryptoProto.DecryptResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CryptoServiceMethodDescriptorSupplier("Decrypt"))
              .build();
        }
      }
    }
    return getDecryptMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CryptoServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CryptoServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CryptoServiceStub>() {
        @Override
        public CryptoServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CryptoServiceStub(channel, callOptions);
        }
      };
    return CryptoServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CryptoServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CryptoServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CryptoServiceBlockingStub>() {
        @Override
        public CryptoServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CryptoServiceBlockingStub(channel, callOptions);
        }
      };
    return CryptoServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CryptoServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CryptoServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CryptoServiceFutureStub>() {
        @Override
        public CryptoServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CryptoServiceFutureStub(channel, callOptions);
        }
      };
    return CryptoServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void encrypt(CryptoProto.EncryptRequest request,
                         io.grpc.stub.StreamObserver<CryptoProto.EncryptResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEncryptMethod(), responseObserver);
    }

    /**
     */
    default void decrypt(CryptoProto.DecryptRequest request,
                         io.grpc.stub.StreamObserver<CryptoProto.DecryptResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDecryptMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service CryptoService.
   */
  public static abstract class CryptoServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return CryptoServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service CryptoService.
   */
  public static final class CryptoServiceStub
      extends io.grpc.stub.AbstractAsyncStub<CryptoServiceStub> {
    private CryptoServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected CryptoServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CryptoServiceStub(channel, callOptions);
    }

    /**
     */
    public void encrypt(CryptoProto.EncryptRequest request,
                        io.grpc.stub.StreamObserver<CryptoProto.EncryptResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEncryptMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void decrypt(CryptoProto.DecryptRequest request,
                        io.grpc.stub.StreamObserver<CryptoProto.DecryptResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDecryptMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service CryptoService.
   */
  public static final class CryptoServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<CryptoServiceBlockingStub> {
    private CryptoServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected CryptoServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CryptoServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public CryptoProto.EncryptResponse encrypt(CryptoProto.EncryptRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEncryptMethod(), getCallOptions(), request);
    }

    /**
     */
    public CryptoProto.DecryptResponse decrypt(CryptoProto.DecryptRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDecryptMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service CryptoService.
   */
  public static final class CryptoServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<CryptoServiceFutureStub> {
    private CryptoServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected CryptoServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CryptoServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<CryptoProto.EncryptResponse> encrypt(
        CryptoProto.EncryptRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEncryptMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<CryptoProto.DecryptResponse> decrypt(
        CryptoProto.DecryptRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDecryptMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ENCRYPT = 0;
  private static final int METHODID_DECRYPT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ENCRYPT:
          serviceImpl.encrypt((CryptoProto.EncryptRequest) request,
              (io.grpc.stub.StreamObserver<CryptoProto.EncryptResponse>) responseObserver);
          break;
        case METHODID_DECRYPT:
          serviceImpl.decrypt((CryptoProto.DecryptRequest) request,
              (io.grpc.stub.StreamObserver<CryptoProto.DecryptResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getEncryptMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              CryptoProto.EncryptRequest,
              CryptoProto.EncryptResponse>(
                service, METHODID_ENCRYPT)))
        .addMethod(
          getDecryptMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              CryptoProto.DecryptRequest,
              CryptoProto.DecryptResponse>(
                service, METHODID_DECRYPT)))
        .build();
  }

  private static abstract class CryptoServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CryptoServiceBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return CryptoProto.getDescriptor();
    }

    @Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CryptoService");
    }
  }

  private static final class CryptoServiceFileDescriptorSupplier
      extends CryptoServiceBaseDescriptorSupplier {
    CryptoServiceFileDescriptorSupplier() {}
  }

  private static final class CryptoServiceMethodDescriptorSupplier
      extends CryptoServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CryptoServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (CryptoServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CryptoServiceFileDescriptorSupplier())
              .addMethod(getEncryptMethod())
              .addMethod(getDecryptMethod())
              .build();
        }
      }
    }
    return result;
  }
}
