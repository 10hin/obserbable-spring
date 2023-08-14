package com.example.demo.web;

import io.micrometer.tracing.Tracer;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/")
public class MainController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainController.class);

    private final WebClient loopbackClient;
    private final Tracer tracer;

    public MainController(
            final WebClient.Builder clientBuilder,
            @org.springframework.beans.factory.annotation.Value("${server.port:8080}")
            final int serverPort,
            final Tracer tracer
    ) {
        this.loopbackClient = clientBuilder.baseUrl("http://localhost:"+serverPort)
                .build();
        this.tracer = tracer;
    }

    @GetMapping(consumes = {MediaType.ALL_VALUE})
    public Mono<ResponseEntity<Void>> index(
            @RequestHeader(name = "FAILS", required = false, defaultValue = "false")
            final boolean fails,
            @RequestHeader final HttpHeaders headers
    ) {

        LOGGER.info("MainController.index() called");
        LOGGER.debug("FAILS header value: {}", fails);
        LOGGER.debug("headers: {}", headers);
        try {
            if (fails) {
                throw new NullPointerException("dummy: someVariable is null");
            }
            return Mono.just(ResponseEntity.ok().build());
        } catch (Throwable t) {
            LOGGER.error("unexpected error thrown", t);
            throw new InternalError("unexpected error thrown", t);
        }

    }

    @GetMapping("recursive")
    public Mono<ResponseEntity<List<RecursiveCallResult>>> recursive() {

        LOGGER.info("MainController.recursive() called");
        return this.loopbackClient.get()
                .uri("/")
                .accept(MediaType.ALL)
                .retrieve()
                .toEntity(Void.class)
                .map(entity -> ResponseEntity.ok(List.of(new RecursiveCallResult(entity.getStatusCode()))));

    }

    @Value
    public static class RecursiveCallResult {
        HttpStatusCode status;
    }

}
