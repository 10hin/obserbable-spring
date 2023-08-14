package com.example.demo.web;


import com.example.demo.DemoApplication;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/mqtt")
public class MQTTController {
    private static final Logger LOGGER = LoggerFactory.getLogger(MQTTController.class);
    private final DemoApplication.MqttPublishGateway mqttPublishGateway;
    @Autowired
    public MQTTController(
            final DemoApplication.MqttPublishGateway mqttPublishGateway
    ) {
        this.mqttPublishGateway = mqttPublishGateway;
    }
    @PostMapping(path = "/topic1", consumes = {"text/plain"})
    public Mono<ResponseEntity<Void>> publish(
            @RequestBody final Mono<String> body
    ) {
        return body.doOnNext(mqttPublishGateway::sendToMQTT)
                .<ResponseEntity<Void>>map((reqBody) -> ResponseEntity.ok().build())
                .doOnError((error) -> LOGGER.error("failed to pubish mqtt message", error))
                .onErrorResume((error) -> Mono.just(ResponseEntity.internalServerError().build()));
    }
    @Data
    public static class MQTTMessage {
        private String message;
        @JsonCreator
        public MQTTMessage() {}
    }
}
