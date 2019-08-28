package com.rbkmoney.shumpune.service;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumpune.domain.BalanceModel;
import org.apache.thrift.TException;

public interface PostingPlanService {

    Clock hold(PostingPlanChange postingPlanChange) throws TException;

    Clock commit(PostingPlan postingPlan) throws TException;

    Clock rollback(PostingPlan postingPlan) throws TException;

    BalanceModel getBalanceById(Long id, Clock clock) throws TException;

}
