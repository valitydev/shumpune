package com.rbkmoney.shumpune.dao;

import com.rbkmoney.damsel.shumpune.AccountPrototype;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.shumpune.dao.mapper.AccountSnapshotMapper;
import com.rbkmoney.shumpune.domain.AccountSnapshot;
import com.rbkmoney.shumpune.exception.DaoException;
import org.springframework.core.NestedRuntimeException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Service
public class AccountDaoImpl extends NamedParameterJdbcDaoSupport implements AccountDao {

    private final RowMapper<AccountSnapshot> accountMapper;

    public AccountDaoImpl(DataSource ds, AccountSnapshotMapper accountMapper) {
        setDataSource(ds);
        this.accountMapper = accountMapper;
    }

    @Override
    public Long insert(AccountPrototype prototype) {
        final String sql = "INSERT INTO shm.account(curr_sym_code, creation_time, description) " +
                "VALUES (:curr_sym_code, :creation_time, :description) RETURNING id;";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("curr_sym_code", prototype.getCurrencySymCode());
        params.addValue("creation_time", getInstant(prototype), Types.OTHER);
        params.addValue("description", prototype.getDescription());
        try {
            GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
            int updateCount = getNamedParameterJdbcTemplate()
                    .update(sql, params, keyHolder);
            if (updateCount != 1) {
                throw new DaoException("Account creation returned unexpected update count: " + updateCount);
            }
            return keyHolder.getKey().longValue();
        } catch (NestedRuntimeException e) {
            throw new DaoException(e);
        }
    }

    private LocalDateTime getInstant(AccountPrototype prototype) {
        return toLocalDateTime(prototype.isSetCreationTime() ? TypeUtil.stringToInstant(prototype.getCreationTime()) : Instant.now());
    }

    private LocalDateTime toLocalDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    @Override
    public AccountSnapshot getSnapshotById(Long id, Long clock) {
        if (id != null && clock != null) {
            final String sql = "select ac.*, al.id as clock, al.own_accumulated, al.max_accumulated, al.min_accumulated from" +
                    " (select t1.id, t1.curr_sym_code from shm.account t1 where t1.id = any(ids)) ac LEFT JOIN" +
                    " (select t2.id, t2.account_id, t2.own_accumulated, t2.max_accumulated, t2.min_accumulated from shm.account_log t2) al" +
                    " on ac.id = al.account_id and al.id = (select max(t3.id) from shm.account_log t3  where t3.account_id=ac.id)";
            try {
                AccountSnapshot accountSnapshot = getJdbcTemplate()
                        .queryForObject(sql, accountMapper);
                return accountSnapshot;
            } catch (NestedRuntimeException e) {
                throw new DaoException(e);
            }
        }
        return null;
    }

}
