package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.damsel.shumpune.Account;
import com.rbkmoney.shumpune.constant.AccountLogFields;
import com.rbkmoney.shumpune.utils.ResultSetMapperUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;

@Service
public class AccountMapper implements RowMapper<Account> {

    @Override
    public Account mapRow(ResultSet rs, int i) throws SQLException {
        return new Account()
                .setId(rs.getLong(AccountLogFields.ID))
                .setCurrencySymCode(rs.getString(AccountLogFields.CURR_SYM_CODE))
                .setCreationTime(ResultSetMapperUtils.getTimeWithConvertToString(rs, AccountLogFields.CREATION_TIME))
                .setDescription(rs.getString(AccountLogFields.DESCRIPTION));
    }

}