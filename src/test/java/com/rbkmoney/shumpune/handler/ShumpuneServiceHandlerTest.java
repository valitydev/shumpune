package com.rbkmoney.shumpune.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.shumpune.DaoTestBase;
import com.rbkmoney.shumpune.ShumpuneApplication;
import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.dao.AccountDao;
import com.rbkmoney.shumpune.dao.PlanDao;
import com.rbkmoney.shumpune.domain.BalanceModel;
import com.rbkmoney.shumpune.utils.AccountGenerator;
import com.rbkmoney.shumpune.utils.PostingGenerator;
import com.rbkmoney.shumpune.utils.VectorClockSerializer;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.util.ArrayList;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ShumpuneApplication.class)
public class ShumpuneServiceHandlerTest extends DaoTestBase {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    AccountDao accountDao;

    @Autowired
    ShumpuneServiceHandler handler;

    @Autowired
    PlanDao planDao;

    @Test(expected = TException.class)
    public void holdAccountNotFountError() throws TException {
        PostingPlanChange postingPlanChange = PostingGenerator.createPostingPlanChange("plan_hold_error", 1111L, 21L, 22222L);
        handler.hold(postingPlanChange);
    }

    @Test
    public void hold() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = AccountGenerator.createAccountPrototype(now);
        long providerAcc = handler.createAccount(accountPrototype);
        long merchantAcc = handler.createAccount(accountPrototype);
        long systemAcc = handler.createAccount(accountPrototype);

        String planHold = "plan_hold";
        PostingPlanChange postingPlanChange = PostingGenerator.createPostingPlanChange(planHold, providerAcc, systemAcc, merchantAcc);
        Clock clock = handler.hold(postingPlanChange);

        jdbcTemplate.query("select * from shm.plan_log where plan_id = \'" + postingPlanChange.getId() + "\'",
                (rs, rowNum) -> {
                    Assert.assertEquals(PostingOperation.HOLD.name(), rs.getString("last_operation"));
                    Assert.assertEquals(postingPlanChange.getBatch().getId(), rs.getLong("last_batch_id"));
                    Assert.assertTrue(rs.getLong("clock") > 0);
                    return null;
                });

        checkMinAvailable(providerAcc, -294000L, merchantAcc, 291000L, systemAcc, 3000L, clock);
    }

    private void checkMinAvailable(long providerAcc, long providerSum, long merchantAcc, long merchantSum,
                                   long systemAcc, long systemSum, Clock clock) {
        BalanceModel balance = planDao.getBalance(providerAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(providerSum, balance.getMinAvailableAmount().longValue());

        BalanceModel balanceMerch = planDao.getBalance(merchantAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(merchantSum, balanceMerch.getMinAvailableAmount().longValue());

        BalanceModel balanceSystem = planDao.getBalance(systemAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(systemSum, balanceSystem.getMinAvailableAmount().longValue());
    }

    @Test(expected = TException.class)
    public void commitError() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = AccountGenerator.createAccountPrototype(now);
        long providerAcc = handler.createAccount(accountPrototype);
        long merchantAcc = handler.createAccount(accountPrototype);
        long systemAcc = handler.createAccount(accountPrototype);

        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc);
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        batchList.add(batch);
        PostingPlan postingPlan = new PostingPlan()
                .setId("plan_commit_error")
                .setBatchList(batchList);

        handler.commitPlan(postingPlan);
    }

    @Test
    public void commit() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = AccountGenerator.createAccountPrototype(now);
        long providerAcc = handler.createAccount(accountPrototype);
        long merchantAcc = handler.createAccount(accountPrototype);
        long systemAcc = handler.createAccount(accountPrototype);

        String planCommit = "planCommit";
        PostingPlanChange postingPlanChange = PostingGenerator.createPostingPlanChange(planCommit, providerAcc, systemAcc, merchantAcc);
        handler.hold(postingPlanChange);

        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc);
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        batchList.add(batch);
        PostingPlan postingPlan = new PostingPlan()
                .setId(planCommit)
                .setBatchList(batchList);

        Clock clock = handler.commitPlan(postingPlan);

        jdbcTemplate.query("select * from shm.plan_log where plan_id = \'" + postingPlan.getId() + "\'",
                (rs, rowNum) -> {
                    Assert.assertEquals(PostingOperation.COMMIT.name(), rs.getString("last_operation"));
                    return rs.getString("last_operation");
                });

        checkMinAvailable(providerAcc, -294000L, merchantAcc, 291000L, systemAcc, 3000L, clock);
        checkOwnAcc(providerAcc, -294000L, merchantAcc, 291000L, systemAcc, 3000L, clock);
    }

    private void checkOwnAcc(long providerAcc, long providerSum, long merchantAcc, long merchantSum,
                             long systemAcc, long systemSum, Clock clock) {
        BalanceModel balanceOwn = planDao.getBalance(providerAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(providerSum, balanceOwn.getOwnAmount().longValue());

        BalanceModel balanceMerchOwn = planDao.getBalance(merchantAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(merchantSum, balanceMerchOwn.getOwnAmount().longValue());

        BalanceModel balanceSystemOwn = planDao.getBalance(systemAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(systemSum, balanceSystemOwn.getOwnAmount().longValue());
    }

    @Test
    public void rollback() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = AccountGenerator.createAccountPrototype(now);
        long providerAcc = handler.createAccount(accountPrototype);
        long merchantAcc = handler.createAccount(accountPrototype);
        long systemAcc = handler.createAccount(accountPrototype);

        String planRollbak = "planRollbak";
        PostingPlanChange postingPlanChange = PostingGenerator.createPostingPlanChange(planRollbak, providerAcc, systemAcc, merchantAcc);
        handler.hold(postingPlanChange);

        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc);
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        batchList.add(batch);
        PostingPlan postingPlan = new PostingPlan()
                .setId(planRollbak)
                .setBatchList(batchList);

        Clock clock = handler.rollbackPlan(postingPlan);

        jdbcTemplate.query("select * from shm.plan_log where plan_id = \'" + postingPlan.getId() + "\'",
                (rs, rowNum) -> {
                    Assert.assertEquals(PostingOperation.ROLLBACK.name(), rs.getString("last_operation"));
                    return rs.getString("last_operation");
                });

        checkMinAvailable(providerAcc, 0L, merchantAcc, 0L, systemAcc, 0L, clock);
        checkOwnAcc(providerAcc, 0L, merchantAcc, 0L, systemAcc, 0L, clock);
    }

    @Test
    public void createAccount() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = AccountGenerator.createAccountPrototype(now);
        long accountId = handler.createAccount(accountPrototype);
        Account account = handler.getAccountByID(accountId);
        assertAccount(account, accountPrototype);
        Assert.assertEquals(now.toString(), account.getCreationTime());

        //save without creation_time
        AccountPrototype accountPrototypeWithoutCreationTime = AccountGenerator.createAccountPrototype(null);
        accountId = handler.createAccount(accountPrototypeWithoutCreationTime);
        account = handler.getAccountByID(accountId);
        assertAccount(account, accountPrototype);
        Assert.assertTrue(TypeUtil.stringToInstant(account.getCreationTime()).isAfter(now));
    }

    private void assertAccount(Account account, AccountPrototype accountPrototype) {
        Assert.assertEquals(accountPrototype.getCurrencySymCode(), account.getCurrencySymCode());
        Assert.assertEquals(accountPrototype.getDescription(), account.getDescription());
    }

}