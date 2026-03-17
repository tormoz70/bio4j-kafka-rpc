package ru.sbrf.uamc.kafkarpc.example;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EchoController {

    private final EchoStubProvider stubProvider;

    public EchoController(EchoStubProvider stubProvider) {
        this.stubProvider = stubProvider;
    }

    @GetMapping("/echo")
    public String echo(@RequestParam(defaultValue = "hello") String message) throws Exception {
        var response = stubProvider.getStub().echo(EchoRequest.newBuilder().setMessage(message).build());
        return response.getMessage();
    }
}
