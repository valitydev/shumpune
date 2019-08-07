package com.rbkmoney.shumpune;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ShumpuneApplication.class)
public class ShumpuneApplicationTest extends DaoTestBase {

    @Test
    public void contextLoads() {}
}
