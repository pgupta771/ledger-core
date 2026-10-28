package dev.ledger.domain;

/**
 * Thrown when a requested account or transfer aggregate does not exist.
 */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
