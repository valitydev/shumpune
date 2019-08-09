package com.rbkmoney.shumpune.dao;

import com.rbkmoney.shumpune.domain.BalanceModel;
import com.rbkmoney.shumpune.domain.PostingModel;
import com.rbkmoney.shumpune.domain.PostingPlanModel;
import com.rbkmoney.shumpune.exception.DaoException;

import java.util.List;

public interface PlanDao {

    PostingPlanModel addOrUpdatePlanLog(PostingPlanModel planLog) throws DaoException;

    long insertPostings(List<PostingModel> postingModels);

    BalanceModel getBalance(Long accountId, Long fromClock, Long toClock);

}
