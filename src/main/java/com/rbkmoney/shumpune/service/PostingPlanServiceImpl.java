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
import com.rbkmoney.shumpune.dao.AccountLogDao;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class PostingPlanServiceImpl implements PostingPlanService {

    private final PostingPlanToPostingPlanModelConverter postingPlanToPostingPlanModelConverter;
    private final PlanDaoImpl planDao;
    private final AccountLogDao accountLogDao;
    private final FinalOpValidator finalOpValidator;
    private final PostingBatchValidator postingBatchValidator;
    private final PostingPlanToPostingPlanInfoConverter postingPlanToPostingPlanInfoConverter;
    private final PostingsUpdateValidator postingsUpdateValidator;
    private final PostingPlanToListPostingModelListConverter postingPlanToListPostingModelListConverter;

    @Override
    @Transactional
    public Clock hold(PostingPlanChange postingPlanChange) throws TException {
        postingBatchValidator.validate(postingPlanChange.getBatch(), postingPlanChange.getId());

        PostingPlanModel postingPlanModel = postingPlanToPostingPlanModelConverter.convert(postingPlanChange, PostingOperation.HOLD);

        long clock = planDao.insertPostings(postingPlanModel.getPostingModels());
        postingPlanModel.getPostingPlanInfo().setClock(clock);
        planDao.addOrUpdatePlanLog(postingPlanModel);

        return Clock.vector(VectorClockSerializer.serialize(postingPlanModel.getPostingPlanInfo().getClock()));
    }

    @Override
    @Transactional
    public Clock commit(PostingPlan postingPlan) throws TException {
        return finalOperation(postingPlan, PostingOperation.COMMIT);
    }

    @Override
    @Transactional
    public Clock rollback(PostingPlan postingPlan) throws TException {
        return finalOperation(postingPlan, PostingOperation.ROLLBACK);
    }

    @Override
    public BalanceModel getBalanceById(Long accountId, Clock clock) throws TException {
        long clockValue = getClockValue(accountId, clock);

        BalanceModel lastBalanceById = accountLogDao.getLastBalanceById(accountId);

        long fromClock = 0L;
        if (lastBalanceById != null) {
            if (lastBalanceById.getClock() >= clockValue) {
                return lastBalanceById;
            }
            fromClock = lastBalanceById.getClock();
        }

        long maxClockForAccount = initMaxClockForAccount(accountId, clock, clockValue);

        BalanceModel balance = planDao.getBalance(accountId, fromClock, maxClockForAccount);
        if (lastBalanceById != null) {
            balance.setOwnAmount(lastBalanceById.getOwnAmount() + balance.getOwnAmount());
            balance.setMaxAvailableAmount(lastBalanceById.getMaxAvailableAmount() + balance.getMaxAvailableAmount());
            balance.setMinAvailableAmount(lastBalanceById.getMinAvailableAmount() + balance.getMinAvailableAmount());
        }

        Long idLog = accountLogDao.insert(balance);

        log.info("Created new balance log with for account: {} with id: {} ownAmount: {} minAmount: {} maxAmount: {} for clock: {}",
                accountId, idLog, balance.getOwnAmount(), balance.getMinAvailableAmount(), balance.getMaxAvailableAmount(), balance.getClock());

        return balance;
    }

    private long initMaxClockForAccount(Long id, Clock clock, long clockValue) {
        long maxClockByAccountId = clockValue;
        if (clock.isSetVector()) {
            maxClockByAccountId = planDao.getMaxClockByAccountId(id);
        }
        return maxClockByAccountId;
    }


    private long getClockValue(Long id, Clock clock) {
        long clockValue;
        if (clock.isSetVector()) {
            clockValue = VectorClockSerializer.deserialize(clock.getVector());
        } else {
            clockValue = planDao.getMaxClockByAccountId(id);
        }
        return clockValue;
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

        long clock = planDao.insertPostings(postingPlanToListPostingModelListConverter.convert(postingPlan, postingOperation));

        PostingPlanInfo newPostingPlanInfo = postingPlanToPostingPlanInfoConverter.convert(postingPlan);
        newPostingPlanInfo.setClock(clock);
        newPostingPlanInfo.setPostingOperation(postingOperation);

        PostingPlanInfo updatePlanLog = planDao.updatePlanLog(newPostingPlanInfo);

        return Clock.vector(VectorClockSerializer.serialize(updatePlanLog.getClock()));
    }

}
