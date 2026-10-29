package dev.ledger.service.api;

import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.domain.ResourceNotFoundException;
import dev.ledger.domain.Transfer;
import dev.ledger.service.idempotency.RequestHasher;
import dev.ledger.service.repository.AccountRepository;
import dev.ledger.service.transfer.ReversalService;
import dev.ledger.service.transfer.StatementPage;
import dev.ledger.service.transfer.StatementService;
import dev.ledger.service.transfer.TransferCommand;
import dev.ledger.service.transfer.TransferCoordinator;
import dev.ledger.service.transfer.TransferEngine;
import dev.ledger.service.transfer.TransferExecutionResult;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * JWT-secured REST controller exposing account, transfer, reversal, and statement endpoints (Section 7.5).
 */
@RestController
@RequestMapping("/api/v1")
public class LedgerController {

    private static final Logger log = LoggerFactory.getLogger(LedgerController.class);

    private final AccountRepository accountRepository;
    private final TransferCoordinator transferCoordinator;
    private final TransferEngine transferEngine;
    private final ReversalService reversalService;
    private final StatementService statementService;

    public LedgerController(
            AccountRepository accountRepository,
            TransferCoordinator transferCoordinator,
            TransferEngine transferEngine,
            ReversalService reversalService,
            StatementService statementService) {
        this.accountRepository = accountRepository;
        this.transferCoordinator = transferCoordinator;
        this.transferEngine = transferEngine;
        this.reversalService = reversalService;
        this.statementService = statementService;
    }

    public record CreateAccountRequest(
            @NotBlank String currency,
            String iban,
            AccountType type,
            Long initialBalanceMinor) {}

    public record CreateTransferRequest(
            @NotNull UUID fromAccountId,
            @NotNull UUID toAccountId,
            @NotNull @Positive Long amountMinor,
            @NotBlank String currency,
            String reference) {}

    @PostMapping("/accounts")
    @PreAuthorize("hasAuthority('SCOPE_ledger:write')")
    public ResponseEntity<Account> createAccount(@Valid @RequestBody CreateAccountRequest request) {
        AccountType accountType = request.type() != null ? request.type() : AccountType.CUSTOMER;
        boolean allowNegative = accountType == AccountType.SYSTEM;
        long initialBalance = request.initialBalanceMinor() != null ? request.initialBalanceMinor() : 0L;
        Account account = accountRepository.createAccount(
                UUID.randomUUID(),
                request.iban(),
                Currency.of(request.currency()),
                accountType,
                allowNegative,
                initialBalance);
        return ResponseEntity.status(HttpStatus.CREATED).body(account);
    }

    @GetMapping("/accounts/{id}")
    @PreAuthorize("hasAuthority('SCOPE_ledger:read')")
    public Account getAccount(@PathVariable UUID id) {
        return accountRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + id));
    }

    @PostMapping("/transfers")
    @PreAuthorize("hasAuthority('SCOPE_ledger:write')")
    public ResponseEntity<Transfer> createTransfer(
            @AuthenticationPrincipal Jwt jwt,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody CreateTransferRequest request) {
        String keyHash = RequestHasher.sha256Hex(idempotencyKey);
        MDC.put("idempotency_key_hash", keyHash);
        try {
            String clientId = jwt.getSubject() != null ? jwt.getSubject() : "anonymous-client";
            TransferCommand cmd = new TransferCommand(
                    clientId,
                    idempotencyKey,
                    request.fromAccountId(),
                    request.toAccountId(),
                    request.amountMinor(),
                    Currency.of(request.currency()),
                    request.reference(),
                    null);
            TransferExecutionResult result = transferCoordinator.execute(cmd);
            MDC.put("transfer_id", result.transfer().id().toString());
            log.info("Processed transfer request (replay={})", result.idempotentReplay());
            HttpStatus status = result.idempotentReplay() ? HttpStatus.OK : HttpStatus.CREATED;
            return ResponseEntity.status(status).body(result.transfer());
        } finally {
            MDC.remove("idempotency_key_hash");
            MDC.remove("transfer_id");
        }
    }

    @GetMapping("/transfers/{id}")
    @PreAuthorize("hasAuthority('SCOPE_ledger:read')")
    public Transfer getTransfer(@PathVariable UUID id) {
        return transferEngine.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Transfer not found: " + id));
    }

    @PostMapping("/transfers/{id}/reversals")
    @PreAuthorize("hasAuthority('SCOPE_ledger:write')")
    public ResponseEntity<Transfer> reverseTransfer(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable UUID id,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        String effectiveKey = (idempotencyKey != null && !idempotencyKey.isBlank())
                ? idempotencyKey
                : "reversal:" + id;
        String keyHash = RequestHasher.sha256Hex(effectiveKey);
        MDC.put("idempotency_key_hash", keyHash);
        try {
            String clientId = jwt.getSubject() != null ? jwt.getSubject() : "anonymous-client";
            TransferExecutionResult result = reversalService.reverseTransfer(clientId, id, effectiveKey);
            MDC.put("transfer_id", result.transfer().id().toString());
            log.info("Processed transfer reversal for originalTransferId={} (replay={})", id, result.idempotentReplay());
            HttpStatus status = result.idempotentReplay() ? HttpStatus.OK : HttpStatus.CREATED;
            return ResponseEntity.status(status).body(result.transfer());
        } finally {
            MDC.remove("idempotency_key_hash");
            MDC.remove("transfer_id");
        }
    }

    @GetMapping("/accounts/{id}/statement")
    @PreAuthorize("hasAuthority('SCOPE_ledger:read')")
    public StatementPage getStatement(
            @PathVariable UUID id,
            @RequestParam(required = false) Long cursor,
            @RequestParam(defaultValue = "50") int limit) {
        return statementService.getStatement(id, cursor, limit);
    }
}
