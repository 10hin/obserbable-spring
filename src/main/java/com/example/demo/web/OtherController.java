package com.example.demo.web;

import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/other/")
public class OtherController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OtherController.class);

    private final Tracer tracer;

    public OtherController(
            final Tracer tracer
    ) {
        this.tracer = tracer;
    }

    @GetMapping
    public Mono<ResponseEntity<Void>> index(
            @RequestHeader(name = "FAILS", required = false, defaultValue = "false")
            final boolean fails
    ) {

        LOGGER.info("OtherController.index() called");
        LOGGER.debug("FAILS header value: {}", fails);
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

}
