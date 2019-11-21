package com.rbkmoney.shumpune.integration;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.shumpune.DaoTestBase;
import com.rbkmoney.shumpune.ShumpuneApplication;
import com.rbkmoney.shumpune.handler.ShumpuneServiceHandler;
import com.rbkmoney.shumpune.utils.AccountGenerator;
import com.rbkmoney.shumpune.utils.PostingGenerator;
import com.rbkmoney.shumpune.utils.VectorClockSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.Repeat;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ShumpuneApplication.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class ConsistencyTest extends DaoTestBase {

    @RequiredArgsConstructor
    private class ExecutePlan implements Callable<Map.Entry<Integer, Balance>> {

        private final AtomicInteger counter;
        private final ShumpuneServiceHandler serviceHandler;
        private final PostingPlanChange postingPlanChange;
        private final RetryTemplate retryTemplate;

        @Override
        public Map.Entry<Integer, Balance> call() throws Exception {
            try {
                Clock hold = retryTemplate.execute(context -> serviceHandler.hold(postingPlanChange));
                Balance balanceByID = retryTemplate.execute(context -> serviceHandler.getBalanceByID(postingPlanChange.getBatch().getPostings().get(0).from_id, hold));
                int i = counter.incrementAndGet();
                return Map.entry(i, balanceByID);
            } catch (TException e) {
                e.printStackTrace();
                log.error("failed", e);
            }
            return null;
        }
    }

    private static final int ATTEMPTS = 800;

    @Autowired
    ShumpuneServiceHandler serviceHandler;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Autowired
    ApplicationContext applicationContext;

    RetryTemplate retryTemplate = getRetryTemplate();

    @NotNull
    private RetryTemplate getRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
//        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(100, Map.of(WUnavailableResultException.class, true));
//        retryTemplate.setRetryPolicy(simpleRetryPolicy);
        retryTemplate.setRetryPolicy(new AlwaysRetryPolicy());
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(100L);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private ExecutorService executorService;

    @Test
    @Repeat(100)
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.BEFORE_METHOD)
    public void doWorkConcurrentlyAndThenCheckTotalBalance() throws TException, ExecutionException, InterruptedException {
        executorService = Executors.newFixedThreadPool(16);

        AccountPrototype accountPrototype = AccountGenerator.createAccountPrototype(Instant.now());
        long account0 = serviceHandler.createAccount(accountPrototype);
        long account1 = serviceHandler.createAccount(accountPrototype);
        long account2 = serviceHandler.createAccount(accountPrototype);

        AtomicInteger atomicInteger = new AtomicInteger(0);

        for (int j = 0; j < ATTEMPTS; j++) {
            PostingPlanChange postingPlanChange = PostingGenerator.createPostingPlanChange(j + "", account0, account1, account2, (long) 1);
            executorService.submit(new ExecutePlan(
                    atomicInteger,
                    serviceHandler,
                    postingPlanChange,
                    retryTemplate)
            );
        }


        executorService.shutdown();
        executorService.awaitTermination(20, TimeUnit.HOURS);

        Balance finalBalance = retryTemplate.execute(c -> serviceHandler.getBalanceByID(account0, Clock.latest(new LatestClock())));
        Assert.assertEquals(ATTEMPTS, atomicInteger.get());
        Assert.assertEquals(-ATTEMPTS, finalBalance.min_available_amount);
        Assert.assertEquals(ATTEMPTS, finalBalance.max_available_amount);
        Assert.assertEquals(0, finalBalance.own_amount);

        jdbcTemplate.execute("delete from shm.account;");
        jdbcTemplate.execute("delete from shm.account_log;");
        jdbcTemplate.execute("delete from shm.posting_log;");
    }

    @Test
    @Repeat(1000)
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.BEFORE_METHOD)
    public void holdsMessedUpConcurrently() throws TException, InterruptedException, ExecutionException {
        executorService = Executors.newFixedThreadPool(16);
        transactionTemplate.setIsolationLevel(8);

        AccountPrototype accountPrototype = AccountGenerator.createAccountPrototype(Instant.now());
        long account0 = serviceHandler.createAccount(accountPrototype);
        long account1 = serviceHandler.createAccount(accountPrototype);
        long account2 = serviceHandler.createAccount(accountPrototype);

        log.info("before transaction manager");
        TransactionSynchronizationManager.setCurrentTransactionIsolationLevel(8);
        log.info("set transaction manager");
        jdbcTemplate.execute("insert into shm.account_log" +
                "(account_id, own_amount, max_available_amount, min_available_amount, clock, creation_time) " +
                "values " +
                String.format("(%d, %d, %d, %d, %d, '2019-11-21T00:00:00'); ", account0, 0, 0, 0, 0)
        );
        log.info("before 1st future");
        CompletableFuture<Void> cf1 = CompletableFuture
                .runAsync(() -> retryTemplate.execute(c -> {
                    transactionTemplate.execute(q -> {
                        log.info("1 insert");
                        jdbcTemplate.execute("INSERT INTO shm.posting_log" +
                                "(id, plan_id, batch_id, from_account_id, to_account_id, creation_time, amount, curr_sym_code, operation) " +
                                "values " +
                                String.format("(%d, %d, %d, %d, %d, '2019-11-21T00:00:00', 1, 'RUB', 'HOLD'), ", 1, 1, 1, account0, account1) +
                                String.format("(%d, %d, %d, %d, %d, '2019-11-21T00:00:00', 1, 'RUB', 'HOLD'), ", 2, 1, 1, account1, account2) +
                                String.format("(%d, %d, %d, %d, %d, '2019-11-21T00:00:00', 1, 'RUB', 'HOLD'); ", 4, 1, 1, account2, account0));
                        log.info("1 insert finished");
                        return null;
                    });
                    return null;
                }), executorService)
                .thenRun(() ->
                        retryTemplate.execute(c -> transactionTemplate.execute(q -> {
                            try {
                                return serviceHandler.getBalanceByID(account0, Clock.vector(VectorClockSerializer.serialize(4)));
                            } catch (TException e) {
                                e.printStackTrace();
                            }
                            return null;
                        }))
                );
        log.info("before 2nd future");
        CompletableFuture<Void> cf2 = CompletableFuture
                .runAsync(() -> retryTemplate.execute(c -> {
                    transactionTemplate.execute(q -> {
                        log.info("2 insert");
                        jdbcTemplate.execute("INSERT INTO shm.posting_log" +
                                "(id, plan_id, batch_id, from_account_id, to_account_id, creation_time, amount, curr_sym_code, operation) " +
                                "values " +
                                String.format("(%d, %d, %d, %d, %d, '2019-11-21T00:00:00', 1, 'RUB', 'HOLD'), ", 3, 1, 1, account0, account1) +
                                String.format("(%d, %d, %d, %d, %d, '2019-11-21T00:00:00', 1, 'RUB', 'HOLD'), ", 5, 1, 1, account1, account2) +
                                String.format("(%d, %d, %d, %d, %d, '2019-11-21T00:00:00', 1, 'RUB', 'HOLD'); ", 6, 1, 1, account2, account0));
                        log.info("2 insert finished");
                        return null;
                    });
                    return null;
                }), executorService)
                .thenRun(() ->
                        retryTemplate.execute(c -> transactionTemplate.execute(q -> {
                            try {
                                return serviceHandler.getBalanceByID(account0, Clock.vector(VectorClockSerializer.serialize(6)));
                            } catch (TException e) {
                                e.printStackTrace();
                            }
                            return null;
                        }))
                );
        log.info("before get");
        CompletableFuture.allOf(cf1, cf2).get();
        log.info("after get");

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        Balance finalBalance = retryTemplate.execute(c -> serviceHandler.getBalanceByID(account0, Clock.latest(new LatestClock())));
        Assert.assertEquals(-2, finalBalance.min_available_amount);
        Assert.assertEquals(2, finalBalance.max_available_amount);
        Assert.assertEquals(0, finalBalance.own_amount);

        jdbcTemplate.execute("delete from shm.account;");
        jdbcTemplate.execute("delete from shm.account_log;");
        jdbcTemplate.execute("delete from shm.posting_log;");
    }
}
