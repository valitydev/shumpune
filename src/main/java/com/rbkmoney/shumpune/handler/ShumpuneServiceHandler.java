package com.rbkmoney.shumpune.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.damsel.shumpune.base.InvalidRequest;
import com.rbkmoney.shumpune.dao.AccountDao;
import com.rbkmoney.shumpune.exception.DaoException;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShumpuneServiceHandler implements AccounterSrv.Iface {

    private final AccountDao accountDao;

    @Override
    public Clock hold(PostingPlanChange postingPlanChange) throws InvalidPostingParams, InvalidRequest, TException {
        return null;
    }

    @Override
    public Clock commitPlan(PostingPlan postingPlan) throws InvalidPostingParams, InvalidRequest, TException {
        return null;
    }

    @Override
    public Clock rollbackPlan(PostingPlan postingPlan) throws InvalidPostingParams, InvalidRequest, TException {
        return null;
    }

    @Override
    public PostingPlan getPlan(String s) throws PlanNotFound, TException {
        return null;
    }

    @Override
    public Account getAccountByID(long l, Clock clock) throws AccountNotFound, ClockInFuture, TException {
        return null;
    }

    @Override
    public long createAccount(AccountPrototype accountPrototype) throws TException {
        log.info("Start createAccount prototype: {}", accountPrototype);
        try {
            Long accountId = accountDao.insert(accountPrototype);
            log.info("Finish createAccount accountId: {}", accountId);
            return accountId;
        } catch (DaoException e) {
            log.error("Failed to create account", e);
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Failed to create account", e);
            throw new TException(e);
        }
    }

}
