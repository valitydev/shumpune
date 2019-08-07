package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.shumpune.domain.AccountSnapshot;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Service
public class AccountSnapshotMapper implements RowMapper<AccountSnapshot> {

    @Override
    public AccountSnapshot mapRow(ResultSet rs, int i) throws SQLException {
        long id = rs.getLong("id");
        String currSymCode = rs.getString("curr_sym_code");
        Instant creationTime = rs.getObject("creation_time", LocalDateTime.class).toInstant(ZoneOffset.UTC);
        String description = rs.getString("description");
        long ownAmount = rs.getLong("own_accumulated");
        long maxAmount = rs.getLong("max_accumulated");
        long minAmount = rs.getLong("min_accumulated");
        long clock = rs.getLong("clock");
        return AccountSnapshot.builder()
                .id(id)
                .ownAmount(ownAmount)
                .maxAvailableAmount(maxAmount)
                .minAvailableAmount(minAmount)
                .currencySymCode(currSymCode)
                .creationTime(creationTime)
                .description(description)
                .clock(clock)
                .build();
    }

}