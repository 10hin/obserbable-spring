package com.example.demo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import lombok.Data;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.observation.ObservationRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.GatewayHeader;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.integration.mqtt.support.MqttMessageConverter;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.messaging.*;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Hooks;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@SpringBootApplication
public class DemoApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

	public static void main(String[] args) {
		Hooks.enableAutomaticContextPropagation();
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	public <T extends ObservationRegistry> ObservationRegistryCustomizer<T> observationRegistryCustomizer() {
		return ObservationThreadLocalAccessor.getInstance()::setObservationRegistry;
	}

	@Bean
	public MessageChannel mqttInputChannel(final ObjectMapper objectMapper, final Tracer tracer) {
		// register message handler wrapping with message-unwrapping handler
		return new DirectChannel() {
			private final ConcurrentMap<MessageHandler, MessageHandler> interceptingHandlers = new ConcurrentHashMap<>();
			@Override
			public boolean subscribe(@NonNull final MessageHandler handler) {
				final MessageHandler messageUnwrappingHandler = (message) -> {
					final Object payload = message.getPayload();
					final MessageWithTrace<?> decodedPayload;
					if (payload instanceof byte[] bytesPayload) {
						try {
							decodedPayload = objectMapper.readValue(bytesPayload, new TypeReference<MessageWithTrace<?>>() {});
						} catch (IOException e) {
							throw new MessagingException("failed to extract trace info from MQT message byte array payload", e);
						}
					} else if (payload instanceof String stringPayload) {
						try {
							decodedPayload = objectMapper.readValue(stringPayload, new TypeReference<MessageWithTrace<?>>() {});
						} catch (JsonProcessingException e) {
							throw new MessagingException("failed to extract trace info from MQTT message string payload", e);
						}
					} else {
						throw new MessagingException("expect MQTT message contains String or byte[] payload, but in actual: " + payload.getClass());
					}
					final String propagatedTraceID = decodedPayload.getTraceId();
					final String propagatedSpanID = decodedPayload.getSpanId();
					final TraceContext propagatedTraceContext = tracer.traceContextBuilder()
							.traceId(propagatedTraceID)
							.spanId(propagatedSpanID)
							.sampled(true)
							.build();
					try (final var ignoredParentScope = tracer.currentTraceContext().newScope(propagatedTraceContext)) {
						final Message<?> originalMessage = MessageBuilder.withPayload(decodedPayload.getPayload())
								.copyHeaders(message.getHeaders())
								.build();
						final var newSpan = tracer.nextSpan()
								.name("mqtt-receive");
						try (final var ignoredNewScope = tracer.withSpan(newSpan.start())) {
							handler.handleMessage(originalMessage);
						} finally {
							newSpan.end();
						}
					}
				};
				final var currentHandler = this.interceptingHandlers.putIfAbsent(handler, messageUnwrappingHandler);
				if (currentHandler != null) {
					return false;
				}
				return super.subscribe(messageUnwrappingHandler);
			}
			@Override
			public boolean unsubscribe(@NonNull final MessageHandler handler) {
				final var interceptingHandler = this.interceptingHandlers.get(handler);
				if (interceptingHandler == null) {
					return false;
				}
				this.interceptingHandlers.remove(handler, interceptingHandler);
				return super.unsubscribe(interceptingHandler);
			}
		};
	}

	@Bean
	public MessageProducer inbound(
			@Qualifier("mqttInputChannel")
			final MessageChannel mqttInputChannel,
			final MqttMessageConverter mqttMessageConverter
	) {
		final var adapter = new MqttPahoMessageDrivenChannelAdapter(
				"tcp://localhost:1883",
				"testclient",
				"topic1",
				"topic2"
		);
		adapter.setCompletionTimeout(Duration.ofSeconds(5).toMillis());
		adapter.setConverter(mqttMessageConverter);
		adapter.setQos(2);
		adapter.setOutputChannel(mqttInputChannel);
		return adapter;
	}

	@Bean
	@ServiceActivator(inputChannel = "mqttInputChannel")
	public MessageHandler handler() {
		// user code when receiving message
		return message -> LOGGER.info("mqttInputChannel received message: {}", message);
	}

	@Bean
	public MqttPahoClientFactory mqttClientFactory() {
		final var factory = new DefaultMqttPahoClientFactory();
		final var options = new MqttConnectOptions();
		options.setServerURIs(new String[] {"tcp://localhost:1883"});
		factory.setConnectionOptions(options);
		return factory;
	}

	@Bean
	@ServiceActivator(inputChannel = "mqttOutboundChannel")
	public MessageHandler mqttOutbound(
			final MqttMessageConverter mqttMessageConverter
	) {
		final var messageHandler = new MqttPahoMessageHandler("testclient-pub", mqttClientFactory());
		messageHandler.setAsync(true);
		messageHandler.setDefaultTopic("topic1");
		messageHandler.setDefaultQos(2);
		messageHandler.setConverter(mqttMessageConverter);
		return messageHandler;
	}

	@Bean
	public MessageChannel mqttOutboundChannel(
			@Autowired
			final Tracer tracer
	) {
		final var chan = new DirectChannel();
		final ConcurrentMap<String, Span> spans = new ConcurrentHashMap<>();
		// wrap message payload with trace-id and span-id to propagate trace context
		chan.addInterceptor(new ChannelInterceptor() {
			@Override
			public Message<?> preSend(@NonNull final Message<?> message, @NonNull final MessageChannel channel) {
				final var newSpan = tracer.nextSpan().name("send-mqtt");
				final var traceID = newSpan.context().traceId();
				final var spanID = newSpan.context().spanId();
				spans.put(spanID, newSpan);
				final var newPayload = new MessageWithTrace<>();
				newPayload.setPayload(message.getPayload());
				newPayload.setTraceId(traceID);
				newPayload.setSpanId(spanID);
				final var newMessage = MessageBuilder.withPayload(newPayload)
						.copyHeaders(message.getHeaders())
						.setHeader("span-id", spanID)
						.build();
				return ChannelInterceptor.super.preSend(newMessage, channel);
			}

			@Override
			public void afterSendCompletion(@NonNull final Message<?> message, @NonNull final MessageChannel channel, boolean sent, @Nullable final Exception ex) {
				ChannelInterceptor.super.afterSendCompletion(message, channel, sent, ex);
				final String spanID = (String) message.getHeaders().get("span-id");
				final var currentSpan = spans.get(spanID);
				currentSpan.end();
			}
		});
		return chan;
	}

	@MessagingGateway(defaultRequestChannel = "mqttOutboundChannel", defaultHeaders = {@GatewayHeader(name = MessageHeaders.CONTENT_TYPE, value = "text/plain")})
	public interface MqttPublishGateway {
		void sendToMQTT(final String data);
	}

	// trace context carrier
	@Data
	public static class MessageWithTrace<T> {
		private T payload;
		private String traceId;
		private String spanId;
		@JsonCreator
		public MessageWithTrace() {}
	}

	// adapt DefaultPahoMessageConverter to MessageWithTrace class message payload
	@Bean
	public MqttMessageConverter mqttMessageConverter(final ObjectMapper objectMapper) {
		return new DefaultPahoMessageConverter() {

			@Override
			public MqttMessage fromMessage(@NonNull final Message<?> message, @NonNull final Class<?> targetClass) {
				final byte[] bytesPayload;
				try {
					bytesPayload = objectMapper.writeValueAsBytes(message.getPayload());
				} catch (final JsonProcessingException e) {
					LOGGER.warn("failed to encode message payload to bytes", e);
					return null;
				}
				final var payloadSerializedMessage = MessageBuilder.withPayload(bytesPayload)
						.copyHeaders(message.getHeaders())
						.build();
				return super.fromMessage(payloadSerializedMessage, targetClass);
			}

			@Override
			public Message<?> toMessage(@NonNull final Object payload, MessageHeaders headers) {
				final var rawPayloadMessage = super.toMessage(payload, headers);
				if (rawPayloadMessage == null) {
					LOGGER.warn("failed to decode MQTT message");
					return null;
				}
				final Object rawPayload = rawPayloadMessage.getPayload();
				final MessageWithTrace<?> decodedPayload;
				if (rawPayload instanceof byte[] bytesPayload) {
					try {
						decodedPayload = objectMapper.readValue(bytesPayload, MessageWithTrace.class);
					} catch (IOException e) {
						LOGGER.warn("failed to decode bytes: {}", bytesPayload, e);
						return null;
					}
				} else if (rawPayload instanceof String stringPayload) {
					try {
						decodedPayload = objectMapper.readValue(stringPayload, MessageWithTrace.class);
					} catch (IOException e) {
						LOGGER.warn("failed to decode string: {}", stringPayload, e);
						return null;
					}
				} else {
					return null;
				}
				return MessageBuilder.withPayload(decodedPayload.getPayload())
						.copyHeaders(rawPayloadMessage.getHeaders())
						.setHeader("trace-id", decodedPayload.getTraceId())
						.setHeader("span-id", decodedPayload.getSpanId())
						.build();
			}
		};
	}

}
