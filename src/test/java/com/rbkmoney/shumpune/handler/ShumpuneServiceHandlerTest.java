package com.rbkmoney.shumpune.handler;

import com.rbkmoney.damsel.shumpune.AccountPrototype;
import com.rbkmoney.shumpune.DaoTestBase;
import com.rbkmoney.shumpune.ShumpuneApplication;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;

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
        AccountPrototype accountPrototype = new AccountPrototype();
        Instant now = Instant.now();
        accountPrototype.setCreationTime(ISO_INSTANT.format(now));
        accountPrototype.setCurrencySymCode("RUB");
        accountPrototype.setDescription("test");

        long accountId = handler.createAccount(accountPrototype);

        jdbcTemplate.query("select * from shm.account where id = " + accountId, (rs, rowNum) -> {
            Assert.assertEquals(accountId, rs.getLong("id"));
            Assert.assertEquals("RUB", rs.getString("curr_sym_code"));
            Assert.assertEquals(ISO_INSTANT.format(now), rs.getString("creation_time"));
            Assert.assertEquals("test", rs.getString("description"));
            return null;
        } );

        AccountPrototype accountPrototypeWithoutCreationTime = new AccountPrototype();
        accountPrototype.setCurrencySymCode("RUB");
        accountPrototype.setDescription("test");

        long accountId2 = handler.createAccount(accountPrototypeWithoutCreationTime);

        jdbcTemplate.query("select * from shm.account where id = " + accountId, (rs, rowNum) -> {
            Assert.assertEquals(accountId2, rs.getLong("id"));
            Assert.assertEquals("RUB", rs.getString("curr_sym_code"));
            Assert.assertTrue(Instant.parse(rs.getString("creation_time")).isAfter(now));
            Assert.assertEquals("test", rs.getString("description"));
            return null;
        } );
    }

    @Test
    public void getAccountByIdTest() {

    }

}