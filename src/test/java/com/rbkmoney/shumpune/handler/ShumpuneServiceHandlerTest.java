package com.rbkmoney.shumpune.handler;

import com.rbkmoney.damsel.shumpune.*;
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
import java.time.ZoneOffset;
import java.util.ArrayList;

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
        PostingPlanChange postingPlanChange = createPostingPanChange(1111L, 21L, 22222L);
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

        PostingPlanChange postingPlanChange = createPostingPanChange(providerAcc, systemAcc, merchantAcc);
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

    @NotNull
    private PostingPlanChange createPostingPanChange(Long providerAcc, Long systemAcc, Long merchantAcc) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
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
        postingPlanChange.setBatch(batch)
                .setId("plan");
        return postingPlanChange;
    }

    @Test
    public void createAccount() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = createAccountPrototype(now);
        long accountId = handler.createAccount(accountPrototype);
        //todo handler.getAccountByID(accountId)
        jdbcTemplate.query("select * from shm.account where id = " + accountId,
                (rs, rowNum) -> assertAccounts(now, accountId, rs));

        //save without creation_time
        AccountPrototype accountPrototypeWithoutCreationTime = createAccountPrototype(null);
        long accountId2 = handler.createAccount(accountPrototypeWithoutCreationTime);
        //todo handler.getAccountByID(accountId2)
        jdbcTemplate.query("select * from shm.account where id = " + accountId2,
                (rs, rowNum) -> assertAccounts(null, accountId2, rs));
    }

    @Test
    public void getBalanceByIdTest() throws TException {
        //new account
        long account = handler.createAccount(createAccountPrototype(null));
        Balance balanceByID = handler.getBalanceByID(account, Clock.latest(new LatestClock()));
        assertZeroBalances(account, balanceByID);
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
                    rs.getTimestamp("creation_time").toLocalDateTime().toInstant(ZoneOffset.UTC));
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