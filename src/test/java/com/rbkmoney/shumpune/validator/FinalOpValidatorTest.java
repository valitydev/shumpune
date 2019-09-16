package com.rbkmoney.shumpune.validator;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.base.InvalidRequest;
import org.junit.Test;

import java.util.ArrayList;

public class FinalOpValidatorTest {

    private FinalOpValidator finalOpValidator = new FinalOpValidator();

    @Test(expected = InvalidRequest.class)
    public void validateEmptyBatch() throws InvalidRequest {
        PostingPlan postingPlan = new PostingPlan();
        postingPlan.setBatchList(new ArrayList<>());
        finalOpValidator.validate(postingPlan);
    }

    @Test(expected = InvalidRequest.class)
    public void validateNoPostingInside() throws InvalidRequest {
        PostingPlan postingPlan = new PostingPlan();
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        PostingBatch postingBatch = new PostingBatch();
        batchList.add(postingBatch);
        postingPlan.setBatchList(batchList);
        finalOpValidator.validate(postingPlan);
    }

    @Test(expected = InvalidRequest.class)
    public void validateDuplicateBatch() throws InvalidRequest {
        PostingPlan postingPlan = new PostingPlan();
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        PostingBatch postingBatch = new PostingBatch();
        long batchId = 1L;
        postingBatch.setId(batchId);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting());
        postingBatch.setPostings(postings);
        batchList.add(postingBatch);
        batchList.add(new PostingBatch()
                .setId(batchId)
                .setPostings(postings));
        postingPlan.setBatchList(batchList);
        finalOpValidator.validate(postingPlan);
    }

    @Test(expected = InvalidRequest.class)
    public void validateMaxMinValue() throws InvalidRequest {
        PostingPlan postingPlan = new PostingPlan();
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        PostingBatch postingBatch = new PostingBatch();
        postingBatch.setId(Long.MAX_VALUE);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting());
        postingBatch.setPostings(postings);
        batchList.add(postingBatch);
        postingPlan.setBatchList(batchList);
        finalOpValidator.validate(postingPlan);
    }
}