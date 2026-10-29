package dev.ledger.service;

import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.service.repository.AccountRepository;
import dev.ledger.service.transfer.ConcurrencyMode;
import dev.ledger.service.transfer.TransferCommand;
import dev.ledger.service.transfer.TransferCoordinator;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
@EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
class L4ContentionBenchmarkIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private TransferCoordinator transferCoordinator;

    @Autowired
    private MeterRegistry meterRegistry;

    record BenchResult(long elapsedMs, long txPerSec, long retries) {}

    @Test
    void benchmarkUniformAndHotAccountContention() throws Exception {
        int transfers = 2_000;
        int threads = 32;

        BenchResult unifPess = runBench(ConcurrencyMode.PESSIMISTIC, false, threads, transfers, "bench-unif-pess");
        BenchResult unifSer = runBench(ConcurrencyMode.SERIALIZABLE, false, threads, transfers, "bench-unif-ser");
        BenchResult hotPess = runBench(ConcurrencyMode.PESSIMISTIC, true, threads, transfers, "bench-hot-pess");
        BenchResult hotSer = runBench(ConcurrencyMode.SERIALIZABLE, true, threads, transfers, "bench-hot-ser");

        Properties props = new Properties();
        props.setProperty("unif.pess.tps", Long.toString(unifPess.txPerSec()));
        props.setProperty("unif.pess.ms", Long.toString(unifPess.elapsedMs()));
        props.setProperty("unif.pess.retries", Long.toString(unifPess.retries()));

        props.setProperty("unif.ser.tps", Long.toString(unifSer.txPerSec()));
        props.setProperty("unif.ser.ms", Long.toString(unifSer.elapsedMs()));
        props.setProperty("unif.ser.retries", Long.toString(unifSer.retries()));

        props.setProperty("hot.pess.tps", Long.toString(hotPess.txPerSec()));
        props.setProperty("hot.pess.ms", Long.toString(hotPess.elapsedMs()));
        props.setProperty("hot.pess.retries", Long.toString(hotPess.retries()));

        props.setProperty("hot.ser.tps", Long.toString(hotSer.txPerSec()));
        props.setProperty("hot.ser.ms", Long.toString(hotSer.elapsedMs()));
        props.setProperty("hot.ser.retries", Long.toString(hotSer.retries()));

        Path out = Path.of("target", "contention-benchmark.properties");
        Files.createDirectories(out.getParent());
        try (var writer = Files.newBufferedWriter(out)) {
            props.store(writer, "L4 Contention Benchmark Results");
        }
    }

    private BenchResult runBench(
            ConcurrencyMode mode, boolean hotAccount, int threads, int totalTransfers, String prefix) throws Exception {
        int accountCount = hotAccount ? 20 : 50;
        List<UUID> ids = new ArrayList<>(accountCount);
        for (int i = 0; i < accountCount; i++) {
            ids.add(accountRepository.createAccount(
                    UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 100_000_000L).id());
        }
        UUID hubAccount = ids.getFirst();

        double retriesBefore = meterRegistry.counter("ledger_lock_retries_total").count();
        AtomicInteger seq = new AtomicInteger(0);
        long start = System.nanoTime();

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Callable<Void>> tasks = new ArrayList<>(threads);
            for (int t = 0; t < threads; t++) {
                tasks.add(() -> {
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    while (true) {
                        int n = seq.incrementAndGet();
                        if (n > totalTransfers) {
                            break;
                        }
                        UUID from;
                        UUID to;
                        if (hotAccount) {
                            UUID spoke = ids.get(1 + rng.nextInt(accountCount - 1));
                            if ((n & 1) == 0) {
                                from = hubAccount;
                                to = spoke;
                            } else {
                                from = spoke;
                                to = hubAccount;
                            }
                        } else {
                            int i1 = rng.nextInt(accountCount);
                            int i2 = rng.nextInt(accountCount - 1);
                            if (i2 >= i1) {
                                i2++;
                            }
                            from = ids.get(i1);
                            to = ids.get(i2);
                        }
                        transferCoordinator.execute(
                                new TransferCommand(prefix, "k-" + n, from, to, 10L, Currency.EUR, "bench", null),
                                mode);
                    }
                    return null;
                });
            }
            for (Future<Void> f : pool.invokeAll(tasks)) {
                f.get();
            }
        }

        long elapsedMs = Math.max(1L, (System.nanoTime() - start) / 1_000_000L);
        long retries = Math.round(meterRegistry.counter("ledger_lock_retries_total").count() - retriesBefore);
        long tps = (totalTransfers * 1000L) / elapsedMs;
        System.out.printf("[benchmark] %s completed: %d tx/s (%d ms, %d retries)%n", prefix, tps, elapsedMs, retries);
        return new BenchResult(elapsedMs, tps, retries);
    }
}
