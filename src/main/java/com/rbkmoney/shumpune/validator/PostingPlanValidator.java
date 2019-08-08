package com.rbkmoney.shumpune.validator;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class PostingPlanValidator {

    private static final String POSTING_PLAN_EMPTY = "Plan ($s) has no batches inside";
    private static final String POSTING_BATCH_EMPTY = "Posting batch (%d) has no postings inside";

    public void validate(PostingPlanChange postingPlanChange) throws InvalidRequest {
        List<Posting> postings = postingPlanChange.getBatch().postings;
        if (postingPlanChange.getBatch() == null) {
            log.warn("Plan {} has not batches inside", postingPlanChange.getId());
            throw new InvalidRequest(Collections.singletonList(String.format(POSTING_PLAN_EMPTY, postingPlanChange.getId())));
        }
        if (postings == null || postings.isEmpty()) {
            log.warn("Batch {} has no postings inside", postingPlanChange.getId());
            throw new InvalidRequest(Collections.singletonList(String.format(POSTING_BATCH_EMPTY, postingPlanChange.getBatch().getId())));
        }
    }

}
