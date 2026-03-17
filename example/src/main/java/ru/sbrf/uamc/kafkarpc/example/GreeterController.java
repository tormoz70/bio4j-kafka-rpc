package ru.sbrf.uamc.kafkarpc.example;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreeterController {

    private final GreeterStubProvider stubProvider;

    public GreeterController(GreeterStubProvider stubProvider) {
        this.stubProvider = stubProvider;
    }

    @GetMapping("/greet")
    public String greet(@RequestParam(defaultValue = "World") String name) throws Exception {
        var response = stubProvider.getStub().getGreeting(GetGreetingRequest.newBuilder().setName(name).build());
        return response.getGreeting();
    }
}
