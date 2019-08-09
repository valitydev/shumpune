package com.rbkmoney.shumpune.dao;

import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.dao.mapper.PostingPlanInfoMapper;
import com.rbkmoney.shumpune.domain.BalanceModel;
import com.rbkmoney.shumpune.domain.PostingModel;
import com.rbkmoney.shumpune.domain.PostingPlanInfo;
import com.rbkmoney.shumpune.domain.PostingPlanModel;
import com.rbkmoney.shumpune.exception.DaoException;
import org.springframework.core.NestedRuntimeException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcDaoSupport;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class PlanDaoImpl extends NamedParameterJdbcDaoSupport implements PlanDao {

    private static final int BATCH_SIZE = 1000;

    private final PostingPlanInfoMapper planRowMapper;

    public PlanDaoImpl(DataSource ds, PostingPlanInfoMapper planRowMapper) {
        setDataSource(ds);
        this.planRowMapper = planRowMapper;
    }

    @Override
    public PostingPlanModel addOrUpdatePlanLog(PostingPlanModel planLog) throws DaoException {
        final String sql =
                "insert into shm.plan_log (plan_id, last_batch_id, clock, last_operation) " +
                        "values (:plan_id, :last_batch_id, :clock, :last_operation::shm.posting_operation_type) " +
                        "on conflict (plan_id) " +
                        "do update set clock=:clock, last_operation=:last_operation::shm.posting_operation_type, last_batch_id=:last_batch_id " +
                        "where shm.plan_log.last_operation=:last_operation::shm.posting_operation_type " +
                        "returning *";

        MapSqlParameterSource params = createParams(planLog.getPostingPlanInfo());
        try {
            PostingPlanInfo postingPlanInfo = getNamedParameterJdbcTemplate().queryForObject(sql, params, planRowMapper);
            planLog.setPostingPlanInfo(postingPlanInfo);
            return planLog;
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (NestedRuntimeException e) {
            throw new DaoException(e);
        }
    }

    private MapSqlParameterSource createParams(PostingPlanInfo planLog) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("plan_id", planLog.getId());
        params.addValue("last_batch_id", planLog.getBatchId());
        params.addValue("last_operation", planLog.getPostingOperation().name());
        params.addValue("clock", planLog.getClock());
        return params;
    }

    @Override
    public long insertPostings(List<PostingModel> postings) {
        final String sql = "INSERT INTO shm.posting_log(plan_id, batch_id, from_account_id, to_account_id, creation_time, amount, curr_sym_code, operation, description) VALUES (?, ?, ?, ?, ?, ?, ?, ?::shm.posting_operation_type, ?)";

        int[][] updateCounts = getJdbcTemplate().batchUpdate(sql, postings, BATCH_SIZE,
                (ps, argument) -> {
                    ps.setString(1, argument.getPlanId());
                    ps.setLong(2, argument.getBatchId());
                    ps.setLong(3, argument.getAccountFromId());
                    ps.setLong(4, argument.getAccountToId());
                    ps.setTimestamp(5, Timestamp.from(argument.getCreationTime()));
                    ps.setLong(6, argument.getAmount());
                    ps.setString(7, argument.getCurrencySymbCode());
                    ps.setString(8, argument.getOperation().name());
                    ps.setString(9, argument.getDescription());
                });
        checkBatchUpdate(updateCounts);
        PostingModel postingModel = postings.get(0);
        return selectMaxClock(postingModel);
    }

    @Override
    public BalanceModel getBalance(Long accountId, Long fromClock, Long toClock) {
        MapSqlParameterSource params = new MapSqlParameterSource("fromClock", fromClock)
                .addValue("toClock", toClock)
                .addValue("acc_id", accountId);

        String sqlGetFrom = "select sum(amount) as own_amount, operation " +
                "from shm.posting_log " +
                "where id > :fromClock and id <= :toClock " +
                "and from_account_id = :acc_id " +
                "GROUP BY operation";

        String sqlSumTo = "select sum(amount) as own_amount, operation " +
                "from shm.posting_log " +
                "where id > :fromClock and id <= :toClock " +
                "and to_account_id = :acc_id " +
                "GROUP BY operation";

        Map<String, Long> sumMapFrom = new HashMap<>();
        getNamedParameterJdbcTemplate().query(sqlGetFrom, params, rs -> {
            sumMapFrom.put(rs.getString("operation"), rs.getLong("own_amount"));
        });

        Map<String, Long> sumMapTo = new HashMap<>();
        getNamedParameterJdbcTemplate().query(sqlSumTo, params, rs -> {
            sumMapTo.put(rs.getString("operation"), rs.getLong("own_amount"));
        });

        return BalanceModel.builder()
                .accountId(accountId)
                .clock(toClock)
                .ownAmount(safeGetSum(sumMapTo, PostingOperation.COMMIT) - safeGetSum(sumMapFrom, PostingOperation.COMMIT))
                .minAvailableAmount(safeGetSum(sumMapTo, PostingOperation.HOLD) - safeGetSum(sumMapTo, PostingOperation.ROLLBACK)
                        - safeGetSum(sumMapFrom, PostingOperation.HOLD) - safeGetSum(sumMapFrom, PostingOperation.ROLLBACK))
                .build();
    }

    private long safeGetSum(Map<String, Long> sumMapFrom, PostingOperation postingOperation) {
        return sumMapFrom.containsKey(postingOperation.name()) ? sumMapFrom.get(postingOperation.name()) : 0L;
    }

    private long selectMaxClock(PostingModel postingModel) {
        MapSqlParameterSource params = new MapSqlParameterSource("planId", postingModel.planId)
                .addValue("batchId", postingModel.batchId);

        String sqlGetClock = "select max(id) as clock " +
                "from shm.posting_log " +
                "where plan_id = :planId and batch_id= :batchId";

        return getNamedParameterJdbcTemplate().queryForObject(sqlGetClock, params, Long.class);
    }

    private void checkBatchUpdate(int[][] updateCounts) {
        boolean checked = false;
        for (int i = 0; i < updateCounts.length; ++i) {
            for (int j = 0; j < updateCounts[i].length; ++j) {
                checked = true;
                if (updateCounts[i][j] != 1) {
                    throw new DaoException("Posting log creation returned unexpected update count: " + updateCounts[i][j]);
                }
            }
        }
        if (!checked) {
            throw new DaoException("Posting log creation returned unexpected update count [0]");
        }
    }
}
