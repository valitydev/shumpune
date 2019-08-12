package com.rbkmoney.shumpune.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.shumpune.DaoTestBase;
import com.rbkmoney.shumpune.ShumpuneApplication;
import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.dao.AccountDao;
import com.rbkmoney.shumpune.dao.PlanDao;
import com.rbkmoney.shumpune.domain.BalanceModel;
import com.rbkmoney.shumpune.utils.VectorClockSerializer;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;

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
        PostingPlanChange postingPlanChange = createPostingPlanChange("plan_hold_error", 1111L, 21L, 22222L);
        handler.hold(postingPlanChange);
    }

    @Test
    public void hold() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = createAccountPrototype(now);
        long providerAcc = handler.createAccount(accountPrototype);
        long merchantAcc = handler.createAccount(accountPrototype);
        long systemAcc = handler.createAccount(accountPrototype);

        String planHold = "plan_hold";
        PostingPlanChange postingPlanChange = createPostingPlanChange(planHold, providerAcc, systemAcc, merchantAcc);
        Clock clock = handler.hold(postingPlanChange);

        jdbcTemplate.query("select * from shm.plan_log where plan_id = \'" + postingPlanChange.getId() + "\'",
                (rs, rowNum) -> {
                    Assert.assertEquals(PostingOperation.HOLD.name(), rs.getString("last_operation"));
                    Assert.assertEquals(postingPlanChange.getBatch().getId(), rs.getLong("last_batch_id"));
                    Assert.assertTrue(rs.getLong("clock") > 0);
                    return null;
                });

        BalanceModel balance = planDao.getBalance(providerAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(-294000L, balance.getMinAvailableAmount().longValue());

        BalanceModel balanceMerch = planDao.getBalance(merchantAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(291000L, balanceMerch.getMinAvailableAmount().longValue());

        BalanceModel balanceSystem = planDao.getBalance(systemAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(3000L, balanceSystem.getMinAvailableAmount().longValue());
    }

    @Test(expected = TException.class)
    public void commitError() throws TException {


        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = createAccountPrototype(now);
        long providerAcc = handler.createAccount(accountPrototype);
        long merchantAcc = handler.createAccount(accountPrototype);
        long systemAcc = handler.createAccount(accountPrototype);

        PostingBatch batch = createBatch(providerAcc, systemAcc, merchantAcc);
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
        AccountPrototype accountPrototype = createAccountPrototype(now);
        long providerAcc = handler.createAccount(accountPrototype);
        long merchantAcc = handler.createAccount(accountPrototype);
        long systemAcc = handler.createAccount(accountPrototype);

        String planCommit = "planCommit";
        PostingPlanChange postingPlanChange = createPostingPlanChange(planCommit, providerAcc, systemAcc, merchantAcc);
        Clock clock = handler.hold(postingPlanChange);

        PostingBatch batch = createBatch(providerAcc, systemAcc, merchantAcc);
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        batchList.add(batch);
        PostingPlan postingPlan = new PostingPlan()
                .setId(planCommit)
                .setBatchList(batchList);

        clock = handler.commitPlan(postingPlan);

        jdbcTemplate.query("select * from shm.plan_log where plan_id = \'" + postingPlan.getId() + "\'",
                (rs, rowNum) -> {
                    Assert.assertEquals(PostingOperation.COMMIT.name(), rs.getString("last_operation"));
                    return rs.getString("last_operation");
                });

        BalanceModel balance = planDao.getBalance(providerAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(-294000L, balance.getMinAvailableAmount().longValue());

        BalanceModel balanceOwn = planDao.getBalance(providerAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(-294000L, balance.getOwnAmount().longValue());

        BalanceModel balanceMerch = planDao.getBalance(merchantAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(291000L, balanceMerch.getMinAvailableAmount().longValue());

        BalanceModel balanceMerchOwn = planDao.getBalance(merchantAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(291000L, balanceMerch.getOwnAmount().longValue());

        BalanceModel balanceSystem = planDao.getBalance(systemAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(3000L, balanceSystem.getMinAvailableAmount().longValue());

        BalanceModel balanceSystemOwn = planDao.getBalance(systemAcc, 0L, VectorClockSerializer.deserialize(clock.getVector()));
        Assert.assertEquals(3000L, balanceSystem.getOwnAmount().longValue());
    }

    @NotNull
    private PostingPlanChange createPostingPlanChange(String planId, Long providerAcc, Long systemAcc, Long merchantAcc) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
        PostingBatch batch = createBatch(providerAcc, systemAcc, merchantAcc);
        postingPlanChange.setBatch(batch)
                .setId(planId);
        return postingPlanChange;
    }

    @NotNull
    private PostingBatch createBatch(Long providerAcc, Long systemAcc, Long merchantAcc) {
        PostingBatch batch = new PostingBatch();
        batch.setId(1L);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(300000)
                .setFromId(providerAcc)
                .setToId(merchantAcc)
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(9000)
                .setFromId(merchantAcc)
                .setToId(systemAcc)
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(6000)
                .setFromId(systemAcc)
                .setToId(providerAcc)
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }

    @Test
    public void createAccount() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = createAccountPrototype(now);
        long accountId = handler.createAccount(accountPrototype);
        Account account = handler.getAccountByID(accountId);
        assertAccount(account, accountPrototype);
        Assert.assertEquals(now.toString(), account.getCreationTime());

        //save without creation_time
        AccountPrototype accountPrototypeWithoutCreationTime = createAccountPrototype(null);
        accountId = handler.createAccount(accountPrototypeWithoutCreationTime);
        account = handler.getAccountByID(accountId);
        assertAccount(account, accountPrototype);
        Assert.assertTrue(TypeUtil.stringToInstant(account.getCreationTime()).isAfter(now));
    }

    private void assertAccount(Account account, AccountPrototype accountPrototype) {
        Assert.assertEquals(accountPrototype.getCurrencySymCode(), account.getCurrencySymCode());
        Assert.assertEquals(accountPrototype.getDescription(), account.getDescription());
    }

    private void assertZeroBalances(long account, Balance balanceByID) {
        Assert.assertEquals(account, balanceByID.getId());
        Assert.assertEquals(0L, balanceByID.getMaxAvailableAmount());
        Assert.assertEquals(0L, balanceByID.getMinAvailableAmount());
        Assert.assertEquals(0L, balanceByID.getOwnAmount());
    }

    private Object assertAccounts(Instant now, long accountId, ResultSet rs) throws SQLException {
        Assert.assertEquals(accountId, rs.getLong("id"));
        Assert.assertEquals("RUB", rs.getString("curr_sym_code"));
        if (now != null)
            Assert.assertEquals(now,
                    rs.getTimestamp("creation_time").toLocalDateTime().toInstant(UTC));
        Assert.assertEquals("test", rs.getString("description"));
        return null;
    }

    @NotNull
    private AccountPrototype createAccountPrototype(Instant now) {
        AccountPrototype accountPrototype = new AccountPrototype();
        accountPrototype.setCreationTime(now == null ? null : ISO_INSTANT.format(now));
        accountPrototype.setCurrencySymCode("RUB");
        accountPrototype.setDescription("test");
        return accountPrototype;
    }

}