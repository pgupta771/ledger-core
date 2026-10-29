package dev.ledger.service.api;

import dev.ledger.domain.CurrencyMismatchException;
import dev.ledger.domain.DomainValidationException;
import dev.ledger.domain.DuplicateReversalException;
import dev.ledger.domain.IdempotencyConflictException;
import dev.ledger.domain.IdempotencyPayloadMismatchException;
import dev.ledger.domain.InsufficientFundsException;
import dev.ledger.domain.ResourceNotFoundException;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Translates exceptions into RFC 9457 Problem Details responses without leaking stack traces.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ResourceNotFoundException.class)
    public ProblemDetail handleNotFound(ResourceNotFoundException ex) {
        return problem(HttpStatus.NOT_FOUND, "resource-not-found", "Resource Not Found", ex.getMessage());
    }

    @ExceptionHandler(InsufficientFundsException.class)
    public ProblemDetail handleInsufficientFunds(InsufficientFundsException ex) {
        ProblemDetail pd = problem(
                HttpStatus.UNPROCESSABLE_ENTITY, "insufficient-funds", "Insufficient Funds", ex.getMessage());
        pd.setProperty("accountId", ex.accountId());
        pd.setProperty("availableMinor", ex.availableMinor());
        pd.setProperty("requestedMinor", ex.requestedMinor());
        return pd;
    }

    @ExceptionHandler({IdempotencyPayloadMismatchException.class, CurrencyMismatchException.class})
    public ProblemDetail handleUnprocessable(DomainValidationException ex) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "unprocessable-request", "Unprocessable Request", ex.getMessage());
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<ProblemDetail> handleIdempotencyConflict(IdempotencyConflictException ex) {
        ProblemDetail pd = problem(
                HttpStatus.CONFLICT, "idempotency-in-flight", "Request In Flight", ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .header(HttpHeaders.RETRY_AFTER, Integer.toString(ex.retryAfterSeconds()))
                .body(pd);
    }

    @ExceptionHandler({DuplicateReversalException.class, DuplicateKeyException.class})
    public ProblemDetail handleConflict(RuntimeException ex) {
        String detail = ex instanceof DuplicateReversalException
                ? ex.getMessage()
                : "A resource with the given unique identifier (e.g., IBAN) already exists";
        return problem(HttpStatus.CONFLICT, "resource-conflict", "Conflict", detail);
    }

    @ExceptionHandler({
        DomainValidationException.class,
        MissingRequestHeaderException.class,
        MethodArgumentNotValidException.class,
        MethodArgumentTypeMismatchException.class,
        HttpMessageNotReadableException.class
    })
    public ProblemDetail handleBadRequest(Exception ex) {
        String detail = switch (ex) {
            case MethodArgumentNotValidException manv -> manv.getBindingResult().getFieldErrors().stream()
                    .map(f -> f.getField() + ": " + f.getDefaultMessage())
                    .findFirst()
                    .orElse("Request validation failed");
            case MissingRequestHeaderException mrh -> "Missing required header: " + mrh.getHeaderName();
            case DomainValidationException dve -> dve.getMessage();
            default -> "Malformed request parameter or payload";
        };
        return problem(HttpStatus.BAD_REQUEST, "invalid-request", "Bad Request", detail);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ProblemDetail handleAccessDenied(AccessDeniedException ex) {
        return problem(HttpStatus.FORBIDDEN, "insufficient-scope", "Forbidden", "Insufficient token scope");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("Unhandled internal exception", ex);
        return problem(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "internal-error",
                "Internal Server Error",
                "An unexpected internal error occurred");
    }

    private static ProblemDetail problem(HttpStatus status, String code, String title, String detail) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
        pd.setTitle(title);
        pd.setType(URI.create("urn:ledger:problem:" + code));
        return pd;
    }
}
