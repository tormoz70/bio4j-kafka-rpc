package ru.sbrf.uamc.kafkarpc.protoc;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.compiler.PluginProtos;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.lang.model.element.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Protoc plugin that generates Kafka RPC stubs and service base from .proto service definitions.
 * Uses JavaPoet for code generation.
 */
public class KafkaRpcGenerator {

    private static final ClassName KAFKA_RPC_CHANNEL = ClassName.get("ru.sbrf.uamc.kafkarpc", "KafkaRpcChannel");
    private static final ClassName KAFKA_RPC_CHANNEL_POOL = ClassName.get("ru.sbrf.uamc.kafkarpc.spring", "KafkaRpcChannelPool");
    private static final ClassName KAFKA_RPC_CONSTANTS = ClassName.get("ru.sbrf.uamc.kafkarpc", "KafkaRpcConstants");
    private static final ClassName COMPONENT = ClassName.get("org.springframework.stereotype", "Component");
    private static final ClassName KAFKA_RPC_SERVER = ClassName.get("ru.sbrf.uamc.kafkarpc", "KafkaRpcServer");
    private static final ClassName KAFKA_RPC_SERVICE = ClassName.get("ru.sbrf.uamc.kafkarpc", "KafkaRpcService");
    private static final ClassName IO_EXCEPTION = ClassName.get("java.io", "IOException");
    private static final ClassName INVALID_PROTOCOL_BUFFER = ClassName.get("com.google.protobuf", "InvalidProtocolBufferException");
    private static final ClassName TIMEOUT_EXCEPTION = ClassName.get("java.util.concurrent", "TimeoutException");
    private static final ClassName COMPLETABLE_FUTURE = ClassName.get("java.util.concurrent", "CompletableFuture");
    private static final ClassName COMPLETION_EXCEPTION = ClassName.get("java.util.concurrent", "CompletionException");
    private static final ClassName UUID = ClassName.get("java.util", "UUID");
    private static final ClassName HASH_MAP = ClassName.get("java.util", "HashMap");
    private static final ClassName MAP = ClassName.get("java.util", "Map");
    private static final ClassName STREAMING_PROCESSOR = ClassName.get("ru.sbrf.uamc.kafkarpc", "StreamingProcessor");
    private static final ClassName STREAM_SINK = ClassName.get("ru.sbrf.uamc.kafkarpc", "StreamSink");
    private static final ClassName STREAM_METHOD_HANDLER = ClassName.get("ru.sbrf.uamc.kafkarpc", "KafkaRpcServer", "StreamMethodHandler");

    public static void main(String[] args) throws Exception {
        var request = PluginProtos.CodeGeneratorRequest.parseFrom(System.in);
        var response = generate(request);
        response.writeTo(System.out);
    }

    static PluginProtos.CodeGeneratorResponse generate(PluginProtos.CodeGeneratorRequest request) {
        var builder = PluginProtos.CodeGeneratorResponse.newBuilder();

        for (String name : request.getFileToGenerateList()) {
            DescriptorProtos.FileDescriptorProto file = null;
            for (DescriptorProtos.FileDescriptorProto f : request.getProtoFileList()) {
                if (f.getName().equals(name)) {
                    file = f;
                    break;
                }
            }
            if (file == null) continue;

            String javaPackage = file.getOptions().getJavaPackage();
            if (javaPackage.isEmpty()) {
                javaPackage = toJavaPackage(file.getPackage());
            }
            String outPkg = javaPackage;

            for (DescriptorProtos.ServiceDescriptorProto service : file.getServiceList()) {
                String className = service.getName() + "KafkaRpc";
                JavaFile javaFile = generateService(file, service, outPkg, javaPackage);
                builder.addFile(PluginProtos.CodeGeneratorResponse.File.newBuilder()
                        .setName(javaPackage.replace('.', '/') + "/" + className + ".java")
                        .setContent(javaFile.toString())
                        .build());
                String providerClassName = service.getName() + "StubProvider";
                JavaFile providerFile = generateStubProvider(service, outPkg);
                builder.addFile(PluginProtos.CodeGeneratorResponse.File.newBuilder()
                        .setName(outPkg.replace('.', '/') + "/" + providerClassName + ".java")
                        .setContent(providerFile.toString())
                        .build());
            }
        }
        return builder.build();
    }

    private static String toJavaPackage(String protoPackage) {
        if (protoPackage.isEmpty()) return "";
        return protoPackage.replace('.', '_');
    }

    private static JavaFile generateService(DescriptorProtos.FileDescriptorProto file,
                                           DescriptorProtos.ServiceDescriptorProto service,
                                           String outPkg, String javaPackage) {
        String mainClassName = service.getName() + "KafkaRpc";

        TypeSpec.Builder mainType = TypeSpec.classBuilder(mainClassName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Client stub for $L service over Kafka.", service.getName())
                .addField(FieldSpec.builder(String.class, "SERVICE_NAME", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                        .initializer("$S", service.getName())
                        .build())
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PRIVATE)
                        .build());

        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            if (isStreamMethod(method)) {
                mainType.addType(buildStreamProcessorClass(method, file, javaPackage));
            }
        }

        TypeSpec stubType = buildStubClass(file, service, javaPackage);
        mainType.addType(stubType);

        TypeSpec asyncStubType = buildAsyncStubClass(file, service, javaPackage);
        mainType.addType(asyncStubType);

        TypeSpec serviceBaseType = buildServiceBaseClass(file, service, javaPackage);
        mainType.addType(serviceBaseType);

        return JavaFile.builder(outPkg, mainType.build())
                .addFileComment("Generated by kafka-rpc-protoc. Do not edit.")
                .build();
    }

    private static JavaFile generateStubProvider(DescriptorProtos.ServiceDescriptorProto service, String outPkg) {
        String providerClassName = service.getName() + "StubProvider";
        String clientName = lowerFirst(service.getName());
        String kafkaRpcClassName = service.getName() + "KafkaRpc";
        ClassName stubType = ClassName.get(outPkg, kafkaRpcClassName, "Stub");
        ClassName asyncStubType = ClassName.get(outPkg, kafkaRpcClassName, "AsyncStub");
        MethodSpec getStub = MethodSpec.methodBuilder("getStub")
                .addModifiers(Modifier.PUBLIC)
                .returns(stubType)
                .addStatement("return getStub($S)", clientName)
                .build();
        MethodSpec getStubNamed = MethodSpec.methodBuilder("getStub")
                .addModifiers(Modifier.PUBLIC)
                .returns(stubType)
                .addParameter(String.class, "clientName")
                .addStatement("if (clientName == null || clientName.isEmpty()) throw new $T($S)", IllegalArgumentException.class, "clientName must be set")
                .addStatement("return new $T(pool.getOrCreate(clientName))", stubType)
                .build();
        MethodSpec getAsyncStub = MethodSpec.methodBuilder("getAsyncStub")
                .addModifiers(Modifier.PUBLIC)
                .returns(asyncStubType)
                .addStatement("return getAsyncStub($S)", clientName)
                .build();
        MethodSpec getAsyncStubNamed = MethodSpec.methodBuilder("getAsyncStub")
                .addModifiers(Modifier.PUBLIC)
                .returns(asyncStubType)
                .addParameter(String.class, "clientName")
                .addStatement("if (clientName == null || clientName.isEmpty()) throw new $T($S)", IllegalArgumentException.class, "clientName must be set")
                .addStatement("return new $T(pool.getOrCreate(clientName))", asyncStubType)
                .build();
        MethodSpec ctor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(KAFKA_RPC_CHANNEL_POOL, "pool")
                .addStatement("this.pool = pool")
                .build();
        TypeSpec providerType = TypeSpec.classBuilder(providerClassName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addAnnotation(AnnotationSpec.builder(COMPONENT).build())
                .addField(KAFKA_RPC_CHANNEL_POOL, "pool", Modifier.PRIVATE, Modifier.FINAL)
                .addJavadoc("Provides $L RPC stub (channel and pool hidden). Inject and call getStub().method(...) or getAsyncStub().methodAsync(...). Use getStub(clientName) / getAsyncStub(clientName) to pick a specific client configuration.", service.getName())
                .addMethod(ctor)
                .addMethod(getStub)
                .addMethod(getStubNamed)
                .addMethod(getAsyncStub)
                .addMethod(getAsyncStubNamed)
                .build();
        return JavaFile.builder(outPkg, providerType)
                .addFileComment("Generated by kafka-rpc-protoc. Do not edit.")
                .build();
    }

    private static TypeSpec buildStreamProcessorClass(DescriptorProtos.MethodDescriptorProto method,
                                                      DescriptorProtos.FileDescriptorProto file,
                                                      String javaPackage) {
        TypeName outputType = typeName(method.getOutputType(), file, javaPackage);
        String processorClassName = method.getName() + "Processor";
        return TypeSpec.classBuilder(processorClassName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.ABSTRACT)
                .addSuperinterface(ParameterizedTypeName.get(STREAMING_PROCESSOR, outputType))
                .addJavadoc("Processor for $L stream. Extend and implement {@link #onMessage(Object)}.", lowerFirst(method.getName()))
                .addMethod(MethodSpec.methodBuilder("onMessage")
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .addParameter(ParameterSpec.builder(outputType, "data").build())
                        .returns(TypeName.VOID)
                        .build())
                .build();
    }

    private static TypeSpec buildStubClass(DescriptorProtos.FileDescriptorProto file,
                                           DescriptorProtos.ServiceDescriptorProto service,
                                           String javaPackage) {
        TypeSpec.Builder stub = TypeSpec.classBuilder("Stub")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Blocking stub.")
                .addField(KAFKA_RPC_CHANNEL, "channel", Modifier.PRIVATE, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(KAFKA_RPC_CHANNEL, "channel")
                        .addStatement("this.channel = channel")
                        .build());

        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            TypeName inputType = typeName(method.getInputType(), file, javaPackage);
            String methodName = lowerFirst(method.getName());
            String fullMethod = service.getName() + "/" + method.getName();

            if (isOneway(method)) {
                MethodSpec onewayMethod = MethodSpec.methodBuilder(methodName)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(TypeName.VOID)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .addException(IO_EXCEPTION)
                        .addStatement("String correlationId = $T.randomUUID().toString()", UUID)
                        .addStatement("channel.send(correlationId, request.toByteArray(), $T.of($T.HEADER_METHOD, $S))", MAP, KAFKA_RPC_CONSTANTS, fullMethod)
                        .build();
                stub.addMethod(onewayMethod);
            } else if (isStreamMethod(method)) {
                TypeName outputType = typeName(method.getOutputType(), file, javaPackage);
                String processorClassName = method.getName() + "Processor";
                ClassName processorType = ClassName.get(javaPackage, service.getName() + "KafkaRpc", processorClassName);
                String streamOrderedValue = isOrderedStream(method) ? "true" : "false";
                MethodSpec streamMethod = MethodSpec.methodBuilder(methodName)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(TypeName.VOID)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .addParameter(ParameterSpec.builder(processorType, "processor").build())
                        .addException(IO_EXCEPTION)
                        .addStatement("String correlationId = $T.randomUUID().toString()", UUID)
                        .addStatement("$T<String, String> headers = $T.of($T.HEADER_METHOD, $S, $T.HEADER_STREAM_ORDERED, $S)", MAP, MAP, KAFKA_RPC_CONSTANTS, fullMethod, KAFKA_RPC_CONSTANTS, streamOrderedValue)
                        .addStatement("channel.startStream(correlationId, request.toByteArray(), headers, new $T<byte[]>() { @Override public void onMessage(byte[] data) { try { processor.onMessage($T.parseFrom(data)); } catch ($T e) { processor.onError(e); } } @Override public void onFinish() { processor.onFinish(); } @Override public void onError($T t) { processor.onError(t); } })", STREAMING_PROCESSOR, outputType, INVALID_PROTOCOL_BUFFER, ClassName.get(Throwable.class))
                        .build();
                stub.addMethod(streamMethod);
            } else {
                TypeName outputType = typeName(method.getOutputType(), file, javaPackage);
                MethodSpec methodBuilder = MethodSpec.methodBuilder(methodName)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(outputType)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .addException(IO_EXCEPTION)
                        .addException(INVALID_PROTOCOL_BUFFER)
                        .addException(TIMEOUT_EXCEPTION)
                        .addStatement("String correlationId = $T.randomUUID().toString()", UUID)
                        .addStatement("byte[] response = channel.request(correlationId, request.toByteArray(), $T.of($T.HEADER_METHOD, $S))", MAP, KAFKA_RPC_CONSTANTS, fullMethod)
                        .addStatement("return $T.parseFrom(response)", outputType)
                        .build();
                stub.addMethod(methodBuilder);
            }
        }

        return stub.build();
    }

    private static TypeSpec buildAsyncStubClass(DescriptorProtos.FileDescriptorProto file,
                                                DescriptorProtos.ServiceDescriptorProto service,
                                                String javaPackage) {
        TypeSpec.Builder asyncStub = TypeSpec.classBuilder("AsyncStub")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Async stub (CompletableFuture).")
                .addField(KAFKA_RPC_CHANNEL, "channel", Modifier.PRIVATE, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(KAFKA_RPC_CHANNEL, "channel")
                        .addStatement("this.channel = channel")
                        .build());

        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            TypeName inputType = typeName(method.getInputType(), file, javaPackage);
            String methodName = lowerFirst(method.getName());
            String fullMethod = service.getName() + "/" + method.getName();

            if (isOneway(method)) {
                TypeName futureVoid = ParameterizedTypeName.get(COMPLETABLE_FUTURE, ClassName.get(Void.class));
                MethodSpec asyncOneway = MethodSpec.methodBuilder(methodName + "Async")
                        .addModifiers(Modifier.PUBLIC)
                        .returns(futureVoid)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .addStatement("String correlationId = $T.randomUUID().toString()", UUID)
                        .addStatement("return $T.runAsync(() -> { try { channel.send(correlationId, request.toByteArray(), $T.of($T.HEADER_METHOD, $S)); } catch ($T e) { throw new $T(e); } })", COMPLETABLE_FUTURE, MAP, KAFKA_RPC_CONSTANTS, fullMethod, IO_EXCEPTION, COMPLETION_EXCEPTION)
                        .build();
                asyncStub.addMethod(asyncOneway);
            } else if (isStreamMethod(method)) {
                TypeName outputType = typeName(method.getOutputType(), file, javaPackage);
                String processorClassName = method.getName() + "Processor";
                ClassName processorType = ClassName.get(javaPackage, service.getName() + "KafkaRpc", processorClassName);
                TypeName futureVoid = ParameterizedTypeName.get(COMPLETABLE_FUTURE, ClassName.get(Void.class));
                ClassName throwable = ClassName.get(Throwable.class);
                String streamOrderedValue = isOrderedStream(method) ? "true" : "false";
                MethodSpec asyncStreamMethod = MethodSpec.methodBuilder(methodName + "Async")
                        .addModifiers(Modifier.PUBLIC)
                        .returns(futureVoid)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .addParameter(ParameterSpec.builder(processorType, "processor").build())
                        .addStatement("$T<Void> future = new $T<>()", COMPLETABLE_FUTURE, COMPLETABLE_FUTURE)
                        .addStatement("String correlationId = $T.randomUUID().toString()", UUID)
                        .addStatement("$T<String, String> headers = $T.of($T.HEADER_METHOD, $S, $T.HEADER_STREAM_ORDERED, $S)", MAP, MAP, KAFKA_RPC_CONSTANTS, fullMethod, KAFKA_RPC_CONSTANTS, streamOrderedValue)
                        .addStatement("$T<byte[]> rawProcessor = new $T<byte[]>() { @Override public void onMessage(byte[] data) { try { processor.onMessage($T.parseFrom(data)); } catch ($T e) { processor.onError(e); } } @Override public void onFinish() { processor.onFinish(); future.complete(null); } @Override public void onError($T t) { processor.onError(t); future.completeExceptionally(t); } }", STREAMING_PROCESSOR, STREAMING_PROCESSOR, outputType, INVALID_PROTOCOL_BUFFER, throwable)
                        .addStatement("try { channel.startStream(correlationId, request.toByteArray(), headers, rawProcessor); } catch ($T e) { future.completeExceptionally(e); }", IO_EXCEPTION)
                        .addStatement("return future")
                        .build();
                asyncStub.addMethod(asyncStreamMethod);
            } else {
                TypeName outputType = typeName(method.getOutputType(), file, javaPackage);
                TypeName futureOutputType = ParameterizedTypeName.get(COMPLETABLE_FUTURE, outputType);
                MethodSpec asyncMethod = MethodSpec.methodBuilder(methodName + "Async")
                        .addModifiers(Modifier.PUBLIC)
                        .returns(futureOutputType)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .addStatement("String correlationId = $T.randomUUID().toString()", UUID)
                        .addStatement("return channel.requestAsync(correlationId, request.toByteArray(), $T.of($T.HEADER_METHOD, $S)).thenApply(response -> { try { return $T.parseFrom(response); } catch ($T e) { throw new $T(e); } })", MAP, KAFKA_RPC_CONSTANTS, fullMethod, outputType, INVALID_PROTOCOL_BUFFER, COMPLETION_EXCEPTION)
                        .build();
                asyncStub.addMethod(asyncMethod);
            }
        }

        return asyncStub.build();
    }

    private static TypeSpec buildServiceBaseClass(DescriptorProtos.FileDescriptorProto file,
                                                  DescriptorProtos.ServiceDescriptorProto service,
                                                  String javaPackage) {
        String defaultServiceName = lowerFirst(service.getName());
        TypeSpec.Builder serviceBase = TypeSpec.classBuilder("ServiceBase")
                .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT, Modifier.STATIC)
                .addSuperinterface(KAFKA_RPC_SERVICE)
                .addJavadoc("Server base - extend and override RPC methods. Service name defaults to $S.\n", defaultServiceName)
                .addField(FieldSpec.builder(String.class, "serviceName", Modifier.PRIVATE, Modifier.FINAL)
                        .build())
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addStatement("this.serviceName = $S", defaultServiceName)
                        .build())
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(String.class, "serviceName")
                        .addStatement("if (serviceName == null || serviceName.isEmpty()) throw new $T($S)", IllegalArgumentException.class, "serviceName must be set")
                        .addStatement("this.serviceName = serviceName")
                        .build())
                .addMethod(MethodSpec.methodBuilder("getServiceName")
                        .addModifiers(Modifier.PUBLIC)
                        .returns(String.class)
                        .addStatement("return serviceName")
                        .build());

        ClassName methodHandler = ClassName.get("ru.sbrf.uamc.kafkarpc", "KafkaRpcServer", "MethodHandler");

        MethodSpec.Builder getHandlers = MethodSpec.methodBuilder("getHandlers")
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(MAP, ClassName.get(String.class), methodHandler))
                .addStatement("$T<String, $T> m = new $T<>()", Map.class, methodHandler, HashMap.class);
        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            if (isStreamMethod(method)) continue;
            String fullMethod = service.getName() + "/" + method.getName();
            getHandlers.addStatement("m.put($S, getHandler($S))", fullMethod, fullMethod);
        }
        getHandlers.addStatement("return m");
        serviceBase.addMethod(getHandlers.build());

        CodeBlock.Builder switchBlock = CodeBlock.builder()
                .add("return (correlationId, request) -> {\n")
                .add("  switch (method) {\n");
        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            if (isStreamMethod(method)) continue;
            TypeName inputType = typeName(method.getInputType(), file, javaPackage);
            String methodName = lowerFirst(method.getName());
            String fullMethod = service.getName() + "/" + method.getName();
            switchBlock.add("    case $S -> {\n", fullMethod);
            switchBlock.add("      $T req;\n", inputType);
            switchBlock.add("      try { req = $T.parseFrom(request); }\n", inputType);
            switchBlock.add("      catch ($T e) { throw new RuntimeException(e); }\n", INVALID_PROTOCOL_BUFFER);
            if (isOneway(method)) {
                switchBlock.add("      $L(req);\n", methodName);
                switchBlock.add("      return null;\n");
            } else {
                switchBlock.add("      return $L(req).toByteArray();\n", methodName);
            }
            switchBlock.add("    }\n");
        }
        switchBlock.add("    default -> throw new $T($S + method);\n", IllegalArgumentException.class, "Unknown method: ");
        switchBlock.add("  }\n");
        switchBlock.add("};\n");

        serviceBase.addMethod(MethodSpec.methodBuilder("getHandler")
                .addModifiers(Modifier.PUBLIC)
                .returns(methodHandler)
                .addParameter(String.class, "method")
                .addCode(switchBlock.build())
                .build());

        MethodSpec.Builder getStreamHandlers = MethodSpec.methodBuilder("getStreamHandlers")
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(MAP, ClassName.get(String.class), STREAM_METHOD_HANDLER))
                .addStatement("$T<String, $T> m = new $T<>()", Map.class, STREAM_METHOD_HANDLER, HashMap.class);
        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            if (!isStreamMethod(method)) continue;
            String fullMethod = service.getName() + "/" + method.getName();
            getStreamHandlers.addStatement("m.put($S, getStreamHandler($S))", fullMethod, fullMethod);
        }
        getStreamHandlers.addStatement("return m");
        serviceBase.addMethod(getStreamHandlers.build());

        CodeBlock.Builder streamSwitchBlock = CodeBlock.builder()
                .add("return (correlationId, request, sink) -> {\n")
                .add("  switch (method) {\n");
        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            if (!isStreamMethod(method)) continue;
            TypeName inputType = typeName(method.getInputType(), file, javaPackage);
            String methodName = lowerFirst(method.getName());
            String fullMethod = service.getName() + "/" + method.getName();
            streamSwitchBlock.add("    case $S -> {\n", fullMethod);
            streamSwitchBlock.add("      $T req;\n", inputType);
            streamSwitchBlock.add("      try { req = $T.parseFrom(request); }\n", inputType);
            streamSwitchBlock.add("      catch ($T e) { throw new RuntimeException(e); }\n", INVALID_PROTOCOL_BUFFER);
            streamSwitchBlock.add("      try { $L(req, sink); } catch ($T e) { throw new RuntimeException(e); }\n", methodName, IO_EXCEPTION);
            streamSwitchBlock.add("    }\n");
        }
        streamSwitchBlock.add("    default -> throw new $T($S + method);\n", IllegalArgumentException.class, "Unknown stream method: ");
        streamSwitchBlock.add("  }\n");
        streamSwitchBlock.add("};\n");

        serviceBase.addMethod(MethodSpec.methodBuilder("getStreamHandler")
                .addModifiers(Modifier.PUBLIC)
                .returns(STREAM_METHOD_HANDLER)
                .addParameter(String.class, "method")
                .addCode(streamSwitchBlock.build())
                .build());

        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            TypeName inputType = typeName(method.getInputType(), file, javaPackage);
            String methodName = lowerFirst(method.getName());
            if (isOneway(method)) {
                serviceBase.addMethod(MethodSpec.methodBuilder(methodName)
                        .addModifiers(Modifier.PROTECTED, Modifier.ABSTRACT)
                        .returns(TypeName.VOID)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .build());
            } else if (isStreamMethod(method)) {
                serviceBase.addMethod(MethodSpec.methodBuilder(methodName)
                        .addModifiers(Modifier.PROTECTED, Modifier.ABSTRACT)
                        .returns(TypeName.VOID)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .addParameter(ParameterSpec.builder(STREAM_SINK, "sink").build())
                        .addException(IO_EXCEPTION)
                        .build());
            } else {
                TypeName outputType = typeName(method.getOutputType(), file, javaPackage);
                serviceBase.addMethod(MethodSpec.methodBuilder(methodName)
                        .addModifiers(Modifier.PROTECTED, Modifier.ABSTRACT)
                        .returns(outputType)
                        .addParameter(ParameterSpec.builder(inputType, "request").build())
                        .build());
            }
        }

        return serviceBase.build();
    }

    private static TypeName typeName(String protoType, DescriptorProtos.FileDescriptorProto file, String javaPackage) {
        String resolved = resolveType(protoType, file, javaPackage);
        int lastDot = resolved.lastIndexOf('.');
        if (lastDot < 0) {
            return ClassName.get(javaPackage, resolved);
        }
        return ClassName.get(resolved.substring(0, lastDot), resolved.substring(lastDot + 1));
    }

    private static String resolveType(String protoType, DescriptorProtos.FileDescriptorProto file, String javaPackage) {
        if (protoType.startsWith(".")) {
            protoType = protoType.substring(1);
        }
        String shortName = protoType.contains(".") ? protoType.substring(protoType.lastIndexOf('.') + 1) : protoType;
        String protoPkg = file.getPackage();
        if (protoType.startsWith(protoPkg) || (protoPkg.isEmpty() && !protoType.contains("."))) {
            return javaPackage + "." + shortName;
        }
        if (protoType.contains(".")) {
            String pkg = protoType.substring(0, protoType.lastIndexOf('.'));
            return pkg.replace('.', '_') + "." + shortName;
        }
        return javaPackage + "." + shortName;
    }

    private static String lowerFirst(String s) {
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }

    /** Oneway: fire-and-forget (no response). Convention: returns google.protobuf.Empty. */
    private static boolean isOneway(DescriptorProtos.MethodDescriptorProto method) {
        String out = method.getOutputType();
        return "google.protobuf.Empty".equals(out) || ".google.protobuf.Empty".equals(out);
    }

    /** Server-streaming: detected via protobuf's server_streaming flag in .proto definition. */
    private static boolean isStreamMethod(DescriptorProtos.MethodDescriptorProto method) {
        return method.getServerStreaming();
    }

    /**
     * Ordered stream: all chunks go to one partition (key=correlationId), message order preserved.
     * Scalable stream: key=null, chunks may go to any partition, multiple consumers can scale; order not guaranteed.
     * Convention: method name starting with "Scalable" = scalable; otherwise = ordered.
     */
    private static boolean isOrderedStream(DescriptorProtos.MethodDescriptorProto method) {
        return !method.getName().startsWith("Scalable");
    }
}
