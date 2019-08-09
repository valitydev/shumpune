package com.rbkmoney.shumpune.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.damsel.shumpune.base.InvalidRequest;
import com.rbkmoney.shumpune.converter.BalanceModelToBalanceConverter;
import com.rbkmoney.shumpune.dao.AccountDao;
import com.rbkmoney.shumpune.domain.BalanceModel;
import com.rbkmoney.shumpune.exception.DaoException;
import com.rbkmoney.shumpune.service.PostingPlanService;
import com.rbkmoney.shumpune.utils.VectorClockSerializer;
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
    private final BalanceModelToBalanceConverter balanceModelToBalanceConverter;
    private final PostingPlanService postingPlanService;

    @Override
    public Clock hold(PostingPlanChange postingPlanChange) throws InvalidPostingParams, InvalidRequest, TException {
        log.info("Start hold postingPlanChange: {}", postingPlanChange);
        try {
            return postingPlanService.hold(postingPlanChange);
        } catch (DaoException e) {
            log.error("Failed to hold e: ", e);
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Failed to hold e: ", e);
            throw new TException(e);
        }
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
    public Account getAccountByID(long l) throws AccountNotFound, TException {
        return null;
    }

    @Override
    public Balance getBalanceByID(long accountId, Clock clock) throws AccountNotFound, ClockInFuture, TException {
        log.info("Start getBalanceByID accountId: {} clock: {}", accountId, clock);
        try {
            Long clockValue = null;
            if (clock.isSetVector()) {
                clockValue = VectorClockSerializer.deserialize(clock.getVector());
            }
            BalanceModel balance = accountDao.getBalanceById(accountId, clockValue);
            log.info("Finish getBalanceByID balance: {}", balance);
            return balanceModelToBalanceConverter.convert(balance);
        } catch (DaoException e) {
            log.error("Failed to getBalanceByID e: ", e);
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Failed to getBalanceByID e: ", e);
            throw new TException(e);
        }
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
