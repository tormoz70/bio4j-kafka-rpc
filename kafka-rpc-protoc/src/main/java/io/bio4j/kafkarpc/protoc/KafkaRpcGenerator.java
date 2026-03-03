package io.bio4j.kafkarpc.protoc;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.compiler.PluginProtos;
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

    private static final ClassName KAFKA_RPC_CHANNEL = ClassName.get("io.bio4j.kafkarpc", "KafkaRpcChannel");
    private static final ClassName KAFKA_RPC_CONSTANTS = ClassName.get("io.bio4j.kafkarpc", "KafkaRpcConstants");
    private static final ClassName KAFKA_RPC_SERVER = ClassName.get("io.bio4j.kafkarpc", "KafkaRpcServer");
    private static final ClassName KAFKA_RPC_SERVICE = ClassName.get("io.bio4j.kafkarpc", "KafkaRpcService");
    private static final ClassName IO_EXCEPTION = ClassName.get("java.io", "IOException");
    private static final ClassName INVALID_PROTOCOL_BUFFER = ClassName.get("com.google.protobuf", "InvalidProtocolBufferException");
    private static final ClassName TIMEOUT_EXCEPTION = ClassName.get("java.util.concurrent", "TimeoutException");
    private static final ClassName UUID = ClassName.get("java.util", "UUID");
    private static final ClassName HASH_MAP = ClassName.get("java.util", "HashMap");
    private static final ClassName MAP = ClassName.get("java.util", "Map");

    public static void main(String[] args) throws Exception {
        var request = PluginProtos.CodeGeneratorRequest.parseFrom(System.in);
        var response = generate(request);
        response.writeTo(System.out);
    }

    static PluginProtos.CodeGeneratorResponse generate(PluginProtos.CodeGeneratorRequest request) {
        var builder = PluginProtos.CodeGeneratorResponse.newBuilder();
        Map<String, String> packageByFile = new HashMap<>();

        for (DescriptorProtos.FileDescriptorProto file : request.getProtoFileList()) {
            String pkg = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
            for (var dep : file.getDependencyList()) {
                packageByFile.put(dep, packageByFile.getOrDefault(dep, ""));
            }
            packageByFile.put(file.getName(), pkg);
        }

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
                String content = javaFile.toString();
                builder.addFile(PluginProtos.CodeGeneratorResponse.File.newBuilder()
                        .setName(javaPackage.replace('.', '/') + "/" + className + ".java")
                        .setContent(content)
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

        // Stub inner class
        TypeSpec stubType = buildStubClass(file, service, javaPackage);
        mainType.addType(stubType);

        // ServiceBase inner class
        TypeSpec serviceBaseType = buildServiceBaseClass(file, service, javaPackage);
        mainType.addType(serviceBaseType);

        return JavaFile.builder(outPkg, mainType.build())
                .addFileComment("Generated by kafka-rpc-protoc. Do not edit.")
                .build();
    }

    private static TypeSpec buildStubClass(DescriptorProtos.FileDescriptorProto file,
                                           DescriptorProtos.ServiceDescriptorProto service,
                                           String javaPackage) {
        TypeSpec.Builder stub = TypeSpec.classBuilder("Stub")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Blocking stub.")
                .addField(KAFKA_RPC_CHANNEL, "channel", Modifier.PRIVATE, Modifier.FINAL)
                .addField(String.class, "requestTopic", Modifier.PRIVATE, Modifier.FINAL)
                .addField(String.class, "replyTopic", Modifier.PRIVATE, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(KAFKA_RPC_CHANNEL, "channel")
                        .addParameter(String.class, "requestTopic")
                        .addParameter(String.class, "replyTopic")
                        .addStatement("this.channel = channel")
                        .addStatement("this.requestTopic = requestTopic")
                        .addStatement("this.replyTopic = replyTopic")
                        .build());

        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            TypeName inputType = typeName(method.getInputType(), file, javaPackage);
            TypeName outputType = typeName(method.getOutputType(), file, javaPackage);
            String methodName = lowerFirst(method.getName());
            String fullMethod = service.getName() + "/" + method.getName();

            MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder(methodName)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(outputType)
                    .addParameter(ParameterSpec.builder(inputType, "request").build())
                    .addException(IO_EXCEPTION)
                    .addException(INVALID_PROTOCOL_BUFFER)
                    .addException(TIMEOUT_EXCEPTION)
                    .addStatement("String correlationId = $T.randomUUID().toString()", UUID)
                    .addStatement("$T<String, String> headers = new $T<>()", MAP, HASH_MAP)
                    .addStatement("headers.put($T.HEADER_METHOD, $S)", KAFKA_RPC_CONSTANTS, fullMethod)
                    .addStatement("byte[] response = channel.request(correlationId, request.toByteArray(), headers)")
                    .addStatement("return $T.parseFrom(response)", outputType);
            stub.addMethod(methodBuilder.build());
        }

        return stub.build();
    }

    private static TypeSpec buildServiceBaseClass(DescriptorProtos.FileDescriptorProto file,
                                                  DescriptorProtos.ServiceDescriptorProto service,
                                                  String javaPackage) {
        TypeSpec.Builder serviceBase = TypeSpec.classBuilder("ServiceBase")
                .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT, Modifier.STATIC)
                .addSuperinterface(KAFKA_RPC_SERVICE)
                .addJavadoc("Server base - extend and override methods. Implements KafkaRpcService for Spring.")
                .addMethod(MethodSpec.methodBuilder("getRequestTopic")
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .returns(String.class)
                        .build())
                .addMethod(MethodSpec.methodBuilder("getReplyTopic")
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .returns(String.class)
                        .build());

        ClassName methodHandler = ClassName.get("io.bio4j.kafkarpc", "KafkaRpcServer", "MethodHandler");

        MethodSpec.Builder getHandlers = MethodSpec.methodBuilder("getHandlers")
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(MAP, ClassName.get(String.class), methodHandler))
                .addStatement("$T<String, $T> m = new $T<>()", Map.class, methodHandler, HashMap.class);
        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            String fullMethod = service.getName() + "/" + method.getName();
            getHandlers.addStatement("m.put($S, getHandler($S))", fullMethod, fullMethod);
        }
        getHandlers.addStatement("return m");
        serviceBase.addMethod(getHandlers.build());

        // getHandler(String method) with switch
        CodeBlock.Builder switchBlock = CodeBlock.builder()
                .add("return (correlationId, request) -> {\n")
                .add("  switch (method) {\n");
        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            TypeName inputType = typeName(method.getInputType(), file, javaPackage);
            String methodName = lowerFirst(method.getName());
            String fullMethod = service.getName() + "/" + method.getName();
            switchBlock.add("    case $S -> {\n", fullMethod);
            switchBlock.add("      $T req;\n", inputType);
            switchBlock.add("      try { req = $T.parseFrom(request); }\n", inputType);
            switchBlock.add("      catch ($T e) { throw new RuntimeException(e); }\n", INVALID_PROTOCOL_BUFFER);
            switchBlock.add("      return $L(req).toByteArray();\n", methodName);
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

        for (DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            TypeName inputType = typeName(method.getInputType(), file, javaPackage);
            TypeName outputType = typeName(method.getOutputType(), file, javaPackage);
            serviceBase.addMethod(MethodSpec.methodBuilder(lowerFirst(method.getName()))
                    .addModifiers(Modifier.PROTECTED, Modifier.ABSTRACT)
                    .returns(outputType)
                    .addParameter(ParameterSpec.builder(inputType, "request").build())
                    .build());
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
}
