package com.rbkmoney.shumpune.handler;

import com.rbkmoney.damsel.shumpune.AccountPrototype;
import com.rbkmoney.shumpune.DaoTestBase;
import com.rbkmoney.shumpune.ShumpuneApplication;
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

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ShumpuneApplication.class)
public class ShumpuneServiceHandlerTest extends DaoTestBase {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ShumpuneServiceHandler handler;

    @Test
    public void createAccount() throws TException {
        Instant now = Instant.now();

        //simple save
        AccountPrototype accountPrototype = createAccountPrototype(now);
        long accountId = handler.createAccount(accountPrototype);
        jdbcTemplate.query("select * from shm.account where id = " + accountId,
                (rs, rowNum) -> assertAccounts(now, accountId, rs));

        //save without creation_time
        AccountPrototype accountPrototypeWithoutCreationTime = createAccountPrototype(null);
        long accountId2 = handler.createAccount(accountPrototypeWithoutCreationTime);
        jdbcTemplate.query("select * from shm.account where id = " + accountId2,
                (rs, rowNum) -> assertAccounts(null, accountId2, rs));
    }

    private Object assertAccounts(Instant now, long accountId, ResultSet rs) throws SQLException {
        Assert.assertEquals(accountId, rs.getLong("id"));
        Assert.assertEquals("RUB", rs.getString("curr_sym_code"));
        if (now != null)
            Assert.assertEquals(now, rs.getTimestamp("creation_time").toLocalDateTime().toInstant(ZoneOffset.UTC));
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

    @Test
    public void getAccountByIdTest() {

    }

}