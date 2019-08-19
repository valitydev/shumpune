package com.rbkmoney.shumpune.dao;

import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.domain.BalanceModel;
import com.rbkmoney.shumpune.domain.PostingModel;
import com.rbkmoney.shumpune.domain.PostingPlanInfo;
import com.rbkmoney.shumpune.domain.PostingPlanModel;

import java.util.List;
import java.util.Map;

public interface PlanDao {

    PostingPlanModel addOrUpdatePlanLog(PostingPlanModel planLog);

    long insertPostings(List<PostingModel> postingModels);

    BalanceModel getBalance(Long accountId, Long fromClock, Long toClock);

    PostingPlanInfo selectForUpdatePlanLog(String planId);

    PostingPlanInfo updatePlanLog(PostingPlanInfo postingPlanInfo);

    List<PostingModel> getPostingModelsPlanById(String planId);

    Map<Long, List<PostingModel>> getPostingLogs(String planId, PostingOperation operation);

    long getMaxClockByAccountId(Long id);
}
