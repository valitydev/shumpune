package com.rbkmoney.shumpune.dao;

import com.rbkmoney.shumpune.domain.PostingModel;
import com.rbkmoney.shumpune.domain.PostingPlanModel;
import com.rbkmoney.shumpune.exception.DaoException;

import java.util.List;

public interface PlanDao {

    PostingPlanModel addOrUpdatePlanLog(PostingPlanModel planLog) throws DaoException;

    void insertPostings(List<PostingModel> postingModels);

}
