package com.rbkmoney.shumpune.validator;


import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.shumpune.domain.PostingModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class PostingsUpdateValidator {

    private static final String POSTING_BATCH_ID_VIOLATION = "Batch has id %d lower than saved id: %d";
    private static final String POSTING_IN_BATCH_INVALID = "Posting in batch id: %d invalid";
    private static final String POSTING_BATCH_SIZE_IS_INCORRECT = "Posting in batch id: %d size: %d but in old size: %d";

    public void validate(PostingPlan receivedPostingPlan, Map<Long, List<PostingModel>> savedBatches) throws TException {
        List<PostingBatch> newBatchList = receivedPostingPlan.getBatchList();
        Set<Long> receivedBatchIds = newBatchList.stream()
                .map(PostingBatch::getId)
                .collect(Collectors.toSet());

        long maxSavedBatch = maxKey(savedBatches);
        long minReceivedBatch = minKey(receivedBatchIds, savedBatches);
        if (maxSavedBatch > minReceivedBatch) {
            log.warn("Batch has id: {} lower than saved id: {}", minReceivedBatch, maxSavedBatch);
            throw new InvalidRequest(Collections.singletonList(String.format(POSTING_BATCH_ID_VIOLATION, minReceivedBatch, maxSavedBatch)));
        }

        for (PostingBatch receivedBatch : newBatchList) {
            List<PostingModel> savedBatch = savedBatches.get(receivedBatch.getId());
            if (savedBatch != null) {
                validateBatchSize(receivedBatch, savedBatch);
                if (!compareBatches(receivedBatch, savedBatch)) {
                    log.warn("Posting in batch id: {} invalid", receivedBatch.getId());
                    throw new InvalidRequest(Collections.singletonList(String.format(POSTING_IN_BATCH_INVALID, receivedBatch.getId())));
                }
            }
        }
    }

    private boolean compareBatches(PostingBatch batch, List<PostingModel> postingModels) {
        return postingModels.stream()
                .allMatch(postingModel -> batch.getPostings().stream()
                        .anyMatch(posting -> compare(postingModel, posting)));
    }

    private void validateBatchSize(PostingBatch batch, List<PostingModel> postingModels) throws InvalidRequest {
        int sizeNew = batch.getPostings() != null ? batch.getPostings().size() : 0;
        int sizeOld = postingModels.size();
        if (sizeNew != sizeOld) {
            log.warn("Posting in batch id: {} size: {} but in old size: {}", batch.getId(), sizeNew, sizeOld);
            throw new InvalidRequest(Collections.singletonList(String.format(POSTING_BATCH_SIZE_IS_INCORRECT, batch.getId(), sizeNew, sizeOld)));
        }
    }

    private boolean compare(PostingModel postingModel, Posting posting) {
        return posting.getAmount() == postingModel.getAmount()
                && posting.getFromId() == postingModel.getAccountFromId()
                && posting.getToId() == postingModel.getAccountToId()
                && posting.getCurrencySymCode().equals(postingModel.getCurrencySymbCode())
                && posting.getDescription().equals(postingModel.getDescription());
    }

    private long minKey(Set<Long> receivedProtocolBatchLogs, Map<Long, List<PostingModel>> postingsModels) {
        return receivedProtocolBatchLogs
                .stream()
                .filter(id -> !postingsModels.containsKey(id))
                .mapToLong(Long::longValue)
                .min()
                .orElse(Long.MAX_VALUE);
    }

    private long maxKey(Map<Long, List<PostingModel>> postingsModels) {
        return postingsModels.keySet().stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(Long.MIN_VALUE);
    }

}
