package com.rbkmoney.shumpune.service;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.converter.PostingPlanToListPostingModelListConverter;
import com.rbkmoney.shumpune.converter.PostingPlanToPostingPlanInfoConverter;
import com.rbkmoney.shumpune.converter.PostingPlanToPostingPlanModelConverter;
import com.rbkmoney.shumpune.dao.AccountDao;
import com.rbkmoney.shumpune.dao.PlanDaoImpl;
import com.rbkmoney.shumpune.domain.BalanceModel;
import com.rbkmoney.shumpune.domain.PostingModel;
import com.rbkmoney.shumpune.domain.PostingPlanInfo;
import com.rbkmoney.shumpune.domain.PostingPlanModel;
import com.rbkmoney.shumpune.utils.VectorClockSerializer;
import com.rbkmoney.shumpune.validator.FinalOpValidator;
import com.rbkmoney.shumpune.validator.PostingBatchValidator;
import com.rbkmoney.shumpune.validator.PostingsUpdateValidator;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class PostingPlanService {

    private final PostingPlanToPostingPlanModelConverter converter;
    private final PlanDaoImpl planDao;
    private final AccountDao accountDao;
    private final FinalOpValidator finalOpValidator;
    private final PostingBatchValidator postingBatchValidator;
    private final PostingPlanToPostingPlanInfoConverter postingPlanToPostingPlanInfoConverter;
    private final PostingsUpdateValidator postingsUpdateValidator;
    private final PostingPlanToListPostingModelListConverter postingPlanToListPostingModelListConverter;

    @Transactional
    public Clock hold(PostingPlanChange postingPlanChange) throws TException {
        postingBatchValidator.validate(postingPlanChange.getBatch(), postingPlanChange.getId());

        PostingPlanModel postingPlanModel = converter.convert(postingPlanChange);

        long clock = planDao.insertPostings(postingPlanModel.getPostingModels());
        postingPlanModel.getPostingPlanInfo().setClock(clock);
        planDao.addOrUpdatePlanLog(postingPlanModel);

        return Clock.vector(VectorClockSerializer.serialize(postingPlanModel.getPostingPlanInfo().getClock()));
    }

    @Transactional
    public Clock commit(PostingPlan postingPlan) throws TException {
        return finalOperation(postingPlan, PostingOperation.COMMIT);
    }

    @Transactional
    public Clock rollback(PostingPlan postingPlan) throws TException {
        return finalOperation(postingPlan, PostingOperation.ROLLBACK);
    }

    @Transactional
    public BalanceModel getBalanceById(Long id, Long clock) throws TException {
        return accountDao.getBalanceById(id, clock);
    }

    private Clock finalOperation(PostingPlan postingPlan, PostingOperation postingOperation) throws TException {
        finalOpValidator.validate(postingPlan);
        for (PostingBatch postingBatch : postingPlan.getBatchList()) {
            postingBatchValidator.validate(postingBatch, postingPlan.getId());
        }

        PostingPlanInfo oldPostingPlanInfo = planDao.selectForUpdatePlanLog(postingPlan.getId());
        if (oldPostingPlanInfo == null) {
            throw new InvalidRequest(Collections.singletonList(String.format("Hold OPERATION not found for plan: %s", postingPlan.getId())));
        }

        Map<Long, List<PostingModel>> postingLogs = planDao.getPostingLogs(oldPostingPlanInfo.getId(), oldPostingPlanInfo.getPostingOperation());
        postingsUpdateValidator.validate(postingPlan, postingLogs);

        long clock = planDao.insertPostings(
                postingPlanToListPostingModelListConverter.convert(postingPlan).stream()
                .peek(p -> p.setOperation(postingOperation))
                        .collect(Collectors.toList())
        );

        PostingPlanInfo newPostingPlanInfo = postingPlanToPostingPlanInfoConverter.convert(postingPlan);
        newPostingPlanInfo.setClock(clock);
        newPostingPlanInfo.setPostingOperation(postingOperation);

        PostingPlanInfo updatePlanLog = planDao.updatePlanLog(newPostingPlanInfo);

        return Clock.vector(VectorClockSerializer.serialize(updatePlanLog.getClock()));
    }

}
