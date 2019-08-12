package com.rbkmoney.shumpune.dao;

import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.dao.mapper.PostingModelMapper;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class PlanDaoImpl extends NamedParameterJdbcDaoSupport implements PlanDao {

    private static final int BATCH_SIZE = 1000;
    public static final String PLAN_ID = "plan_id";
    public static final String LAST_BATCH_ID = "last_batch_id";
    public static final String LAST_OPERATION = "last_operation";
    public static final String CLOCK = "clock";
    public static final String OPERATION = "operation";
    public static final String BATCH_ID = "batchId";
    public static final String SQL_GET_SUM_BY_ACC = "select sum(amount) as own_amount, operation " +
            "from shm.posting_log " +
            "where id > :fromClock and id <= :toClock " +
            "and %s = :acc_id " +
            "GROUP BY operation";

    private final PostingPlanInfoMapper planRowMapper;
    private final PostingModelMapper postingModelMapper;

    public PlanDaoImpl(DataSource ds, PostingPlanInfoMapper planRowMapper, PostingModelMapper postingModelMapper) {
        setDataSource(ds);
        this.planRowMapper = planRowMapper;
        this.postingModelMapper = postingModelMapper;
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
        params.addValue(PLAN_ID, planLog.getId());
        params.addValue(LAST_BATCH_ID, planLog.getBatchId());
        params.addValue(LAST_OPERATION, planLog.getPostingOperation().name());
        params.addValue(CLOCK, planLog.getClock());
        params.addValue("overridable_operation", PostingOperation.HOLD.name());
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
    public Map<Long, List<PostingModel>> getPostingLogs(String planId, PostingOperation operation) {
        final String sql = "select * from shm.posting_log where plan_id = :plan_id and operation = :operation::shm.posting_operation_type";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue(PLAN_ID, planId);
        params.addValue(OPERATION, operation.name());
        try {
            return getNamedParameterJdbcTemplate()
                    .query(sql, params, postingModelMapper).stream()
                    .collect(Collectors.groupingBy(PostingModel::getBatchId));
        } catch (NestedRuntimeException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public BalanceModel getBalance(Long accountId, Long fromClock, Long toClock) {
        MapSqlParameterSource params = new MapSqlParameterSource("fromClock", fromClock)
                .addValue("toClock", toClock)
                .addValue("acc_id", accountId);

        Map<String, Long> sumMapFrom = new HashMap<>();
        getNamedParameterJdbcTemplate()
                .query(String.format(SQL_GET_SUM_BY_ACC, "from_account_id"), params, rs -> {
                    putSumToByOperation(sumMapFrom, rs);
                });

        Map<String, Long> sumMapTo = new HashMap<>();
        getNamedParameterJdbcTemplate()
                .query(String.format(SQL_GET_SUM_BY_ACC, "to_account_id"), params, rs -> {
                    putSumToByOperation(sumMapTo, rs);
        });

        return BalanceModel.builder()
                .accountId(accountId)
                .clock(toClock)
                .ownAmount(safeGetSum(sumMapTo, PostingOperation.COMMIT) - safeGetSum(sumMapFrom, PostingOperation.COMMIT))
                .minAvailableAmount(safeGetSum(sumMapTo, PostingOperation.HOLD) - safeGetSum(sumMapTo, PostingOperation.ROLLBACK)
                        - (safeGetSum(sumMapFrom, PostingOperation.HOLD) - safeGetSum(sumMapFrom, PostingOperation.ROLLBACK)))
                .build();
    }

    private void putSumToByOperation(Map<String, Long> sumMapFrom, ResultSet rs) throws SQLException {
        sumMapFrom.put(rs.getString(OPERATION), rs.getLong("own_amount"));
    }

    @Override
    public PostingPlanInfo selectForUpdatePlanLog(String planId) {
        final String sql = "select * from shm.plan_log where plan_id=:plan_id for update";
        MapSqlParameterSource params = new MapSqlParameterSource(PLAN_ID, planId);
        try {
            return getNamedParameterJdbcTemplate().queryForObject(sql, params, planRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (NestedRuntimeException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public PostingPlanInfo updatePlanLog(PostingPlanInfo postingPlanInfo) {
        final String sql = "update shm.plan_log set clock=:clock, last_operation=:last_operation::shm.posting_operation_type, last_batch_id=:last_batch_id  where plan_id=:plan_id and shm.plan_log.last_operation in (:overridable_operation::shm.posting_operation_type, :same_operation::shm.posting_operation_type) returning *";
        MapSqlParameterSource params = createParams(postingPlanInfo);
        params.addValue("same_operation", postingPlanInfo.getPostingOperation().name());

        try {
            return getNamedParameterJdbcTemplate().queryForObject(sql, params, planRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (NestedRuntimeException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public PostingPlanModel getPostingPlanById(String planId) {
        return null;
    }

    private long safeGetSum(Map<String, Long> sumMapFrom, PostingOperation postingOperation) {
        return sumMapFrom.containsKey(postingOperation.name()) ? sumMapFrom.get(postingOperation.name()) : 0L;
    }

    private long selectMaxClock(PostingModel postingModel) {
        MapSqlParameterSource params = new MapSqlParameterSource("planId", postingModel.getPlanId())
                .addValue("batchId", postingModel.getBatchId());

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
