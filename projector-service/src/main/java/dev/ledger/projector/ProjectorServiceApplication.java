package dev.ledger.projector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Kafka balance read-model projector service.
 */
@SpringBootApplication
public class ProjectorServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProjectorServiceApplication.class, args);
    }
}
