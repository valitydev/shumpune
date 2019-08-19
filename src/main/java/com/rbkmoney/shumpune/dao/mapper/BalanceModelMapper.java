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
        long accountId = rs.getLong(AccountLogFields.ACCOUNT_ID);
        long ownAmount = rs.getLong(AccountLogFields.OWN_AMOUNT);
        long maxAmount = rs.getLong(AccountLogFields.MAX_AVAILABLE_AMOUNT);
        long minAmount = rs.getLong(AccountLogFields.MIN_AVAILABLE_AMOUNT);
        long clock = rs.getLong(AccountLogFields.CLOCK);
        return BalanceModel.builder()
                .accountId(accountId)
                .ownAmount(ownAmount)
                .maxAvailableAmount(maxAmount)
                .minAvailableAmount(minAmount)
                .clock(clock)
                .build();
    }

}