package com.rbkmoney.shumpune.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.damsel.shumpune.base.InvalidRequest;
import org.apache.thrift.TException;

public class ShumpuneServiceHandler implements AccounterSrv.Iface {
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
        return 0;
    }
}
