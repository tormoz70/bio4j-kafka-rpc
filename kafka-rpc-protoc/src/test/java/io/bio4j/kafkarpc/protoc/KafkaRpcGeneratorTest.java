package io.bio4j.kafkarpc.protoc;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.compiler.PluginProtos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaRpcGeneratorTest {

    @Test
    void generatesStubAndServiceBaseForUnaryMethod() {
        var response = generate(serviceWith(
                unaryMethod("GetFoo", ".test.FooRequest", ".test.FooResponse")));

        assertEquals(3, response.getFileCount());
        String kafkaRpcFile = findFile(response, "TestServiceKafkaRpc.java");
        assertNotNull(kafkaRpcFile);
        assertTrue(kafkaRpcFile.contains("class Stub"));
        assertTrue(kafkaRpcFile.contains("class AsyncStub"));
        assertTrue(kafkaRpcFile.contains("class ServiceBase"));
        assertTrue(kafkaRpcFile.contains("getFoo"));
        assertTrue(kafkaRpcFile.contains("getFooAsync"));
        assertTrue(kafkaRpcFile.contains("Map.of(KafkaRpcConstants.HEADER_METHOD"));
    }

    @Test
    void detectsServerStreamingViaProtoFlag() {
        var response = generate(serviceWith(
                streamMethod("StreamBars", ".test.BarRequest", ".test.BarItem")));

        String kafkaRpcFile = findFile(response, "TestServiceKafkaRpc.java");
        assertNotNull(kafkaRpcFile);
        assertTrue(kafkaRpcFile.contains("StreamBarsProcessor"));
        assertTrue(kafkaRpcFile.contains("startStream"));
        assertTrue(kafkaRpcFile.contains("HEADER_STREAM_ORDERED"));
    }

    @Test
    void scalableStreamSetsOrderedFalse() {
        var response = generate(serviceWith(
                streamMethod("ScalableFeed", ".test.FeedRequest", ".test.FeedItem")));

        String kafkaRpcFile = findFile(response, "TestServiceKafkaRpc.java");
        assertNotNull(kafkaRpcFile);
        assertTrue(kafkaRpcFile.contains("\"false\""), "Scalable stream should set ordered=false");
    }

    @Test
    void orderedStreamSetsOrderedTrue() {
        var response = generate(serviceWith(
                streamMethod("StreamItems", ".test.ItemRequest", ".test.ItemData")));

        String kafkaRpcFile = findFile(response, "TestServiceKafkaRpc.java");
        assertNotNull(kafkaRpcFile);
        assertTrue(kafkaRpcFile.contains("\"true\""), "Non-scalable stream should set ordered=true");
    }

    @Test
    void detectsOnewayViaEmptyReturnType() {
        var method = DescriptorProtos.MethodDescriptorProto.newBuilder()
                .setName("Notify")
                .setInputType(".test.NotifyRequest")
                .setOutputType(".google.protobuf.Empty")
                .build();
        var response = generate(serviceWith(method));

        String kafkaRpcFile = findFile(response, "TestServiceKafkaRpc.java");
        assertNotNull(kafkaRpcFile);
        assertTrue(kafkaRpcFile.contains("channel.send("));
        assertTrue(kafkaRpcFile.contains("return null"));
    }

    @Test
    void generatesRpcChannelAndStubProvider() {
        var response = generate(serviceWith(
                unaryMethod("GetFoo", ".test.FooRequest", ".test.FooResponse")));

        assertNotNull(findFile(response, "TestServiceRpcChannel.java"));
        String providerFile = findFile(response, "TestServiceStubProvider.java");
        assertNotNull(providerFile);
        assertTrue(providerFile.contains("getStub"));
        assertTrue(providerFile.contains("getAsyncStub"));
        assertTrue(providerFile.contains("@Component"));
    }

    @Test
    void generatedRpcChannelIsDeprecated() {
        var response = generate(serviceWith(
                unaryMethod("GetFoo", ".test.FooRequest", ".test.FooResponse")));

        String channelFile = findFile(response, "TestServiceRpcChannel.java");
        assertNotNull(channelFile);
        assertTrue(channelFile.contains("@Deprecated"));
    }

    @Test
    void nonStreamingMethodNotDetectedAsStream() {
        var method = DescriptorProtos.MethodDescriptorProto.newBuilder()
                .setName("StreamLikeName")
                .setInputType(".test.Req")
                .setOutputType(".test.Resp")
                .setServerStreaming(false)
                .build();
        var response = generate(serviceWith(method));

        String kafkaRpcFile = findFile(response, "TestServiceKafkaRpc.java");
        assertNotNull(kafkaRpcFile);
        assertFalse(kafkaRpcFile.contains("StreamLikeNameProcessor"),
                "Method with stream-like name but server_streaming=false should not be treated as stream");
    }

    // --- helpers ---

    private static DescriptorProtos.MethodDescriptorProto unaryMethod(String name, String input, String output) {
        return DescriptorProtos.MethodDescriptorProto.newBuilder()
                .setName(name)
                .setInputType(input)
                .setOutputType(output)
                .build();
    }

    private static DescriptorProtos.MethodDescriptorProto streamMethod(String name, String input, String output) {
        return DescriptorProtos.MethodDescriptorProto.newBuilder()
                .setName(name)
                .setInputType(input)
                .setOutputType(output)
                .setServerStreaming(true)
                .build();
    }

    private static DescriptorProtos.ServiceDescriptorProto serviceWith(DescriptorProtos.MethodDescriptorProto... methods) {
        var builder = DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("TestService");
        for (var m : methods) {
            builder.addMethod(m);
        }
        return builder.build();
    }

    private static PluginProtos.CodeGeneratorResponse generate(DescriptorProtos.ServiceDescriptorProto service) {
        var file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test.proto")
                .setPackage("test")
                .setOptions(DescriptorProtos.FileOptions.newBuilder().setJavaPackage("com.test"))
                .addService(service)
                .build();
        var request = PluginProtos.CodeGeneratorRequest.newBuilder()
                .addProtoFile(file)
                .addFileToGenerate("test.proto")
                .build();
        return KafkaRpcGenerator.generate(request);
    }

    private static String findFile(PluginProtos.CodeGeneratorResponse response, String suffix) {
        for (var f : response.getFileList()) {
            if (f.getName().endsWith(suffix)) {
                return f.getContent();
            }
        }
        return null;
    }
}
