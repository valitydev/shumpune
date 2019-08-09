package com.rbkmoney.shumpune.service;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumpune.converter.PostingPlanToPostingPlanModelConverter;
import com.rbkmoney.shumpune.dao.PlanDaoImpl;
import com.rbkmoney.shumpune.domain.PostingPlanModel;
import com.rbkmoney.shumpune.utils.VectorClockSerializer;
import com.rbkmoney.shumpune.validator.PostingBatchValidator;
import com.rbkmoney.shumpune.validator.PostingPlanValidator;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class PostingPlanService {

    private final PostingPlanToPostingPlanModelConverter converter;
    private final PlanDaoImpl planDao;
    private final PostingPlanValidator postingPlanValidator;
    private final PostingBatchValidator postingBatchValidator;

    @Transactional
    public Clock updatePostingPlan(PostingPlanChange postingPlanChange) throws TException {
        postingPlanValidator.validate(postingPlanChange);
        postingBatchValidator.validate(postingPlanChange.getBatch());

        PostingPlanModel postingPlanModel = converter.convert(postingPlanChange);


        long clock = planDao.insertPostings(postingPlanModel.getPostingModels());
        postingPlanModel.getPostingPlanInfo().setClock(clock);
        planDao.addOrUpdatePlanLog(postingPlanModel);

        return Clock.vector(VectorClockSerializer.serialize(postingPlanModel.getPostingPlanInfo().getClock()));
    }

}
