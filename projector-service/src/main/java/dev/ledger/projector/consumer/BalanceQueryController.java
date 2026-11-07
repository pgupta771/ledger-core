package dev.ledger.projector.consumer;

import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only query endpoint returning the asynchronously projected balance for an account.
 */
@RestController
@RequestMapping("/api/v1/balances")
public class BalanceQueryController {

    private final BalanceProjectorService projectorService;

    public BalanceQueryController(BalanceProjectorService projectorService) {
        this.projectorService = projectorService;
    }

    @GetMapping("/{accountId}")
    public ResponseEntity<AccountBalanceView> getProjectedBalance(@PathVariable UUID accountId) {
        AccountBalanceView view = projectorService.snapshotAllBalances().get(accountId);
        return view != null ? ResponseEntity.ok(view) : ResponseEntity.notFound().build();
    }
}
