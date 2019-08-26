package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.shumpune.constant.AccountLogFields;
import com.rbkmoney.shumpune.domain.BalanceModel;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;

@Service
public class BalanceModelMapper implements RowMapper<BalanceModel> {

    @Override
    public BalanceModel mapRow(ResultSet rs, int i) throws SQLException {
        return BalanceModel.builder()
                .accountId(rs.getLong(AccountLogFields.ACCOUNT_ID))
                .ownAmount(rs.getLong(AccountLogFields.OWN_AMOUNT))
                .maxAvailableAmount(rs.getLong(AccountLogFields.MAX_AVAILABLE_AMOUNT))
                .minAvailableAmount(rs.getLong(AccountLogFields.MIN_AVAILABLE_AMOUNT))
                .clock(rs.getLong(AccountLogFields.CLOCK))
                .build();
    }

}