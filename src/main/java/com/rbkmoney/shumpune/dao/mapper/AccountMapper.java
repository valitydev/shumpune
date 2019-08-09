package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.damsel.shumpune.Account;
import com.rbkmoney.geck.common.util.TypeUtil;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Service
public class AccountMapper implements RowMapper<Account> {

    @Override
    public Account mapRow(ResultSet rs, int i) throws SQLException {
        return new Account()
                .setId(rs.getLong("id"))
                .setCurrencySymCode(rs.getString("curr_sym_code"))
                .setCreationTime(getTimeWithConvertToString(rs))
                .setDescription(rs.getString("description"));
    }

    private String getTimeWithConvertToString(ResultSet rs) throws SQLException {
        return TypeUtil.temporalToString(rs.getObject("creation_time", LocalDateTime.class).toInstant(ZoneOffset.UTC));
    }

}