package dev.ledger.domain;

import java.util.UUID;

/**
 * Thrown when attempting to reverse a transfer that has already been reversed.
 */
public class DuplicateReversalException extends DomainValidationException {

    public DuplicateReversalException(UUID transferId) {
        super("Transfer " + transferId + " has already been reversed");
    }
}
