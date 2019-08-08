package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.shumpune.domain.BalanceModel;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;

@Service
public class BalanceModelMapper implements RowMapper<BalanceModel> {

    @Override
    public BalanceModel mapRow(ResultSet rs, int i) throws SQLException {
        long id = rs.getLong("id");
        long ownAmount = rs.getLong("own_amount");
        long maxAmount = rs.getLong("max_available_amount");
        long minAmount = rs.getLong("min_available_amount");
        long clock = rs.getLong("clock");
        return BalanceModel.builder()
                .accountId(id)
                .ownAmount(ownAmount)
                .maxAvailableAmount(maxAmount)
                .minAvailableAmount(minAmount)
                .clock(clock)
                .build();
    }

}