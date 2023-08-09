package com.example.demo;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.observation.ObservationRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Hooks;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		Hooks.enableAutomaticContextPropagation();
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	public <T extends ObservationRegistry> ObservationRegistryCustomizer<T> observationRegistryCustomizer() {
		return ObservationThreadLocalAccessor.getInstance()::setObservationRegistry;
	}

}
