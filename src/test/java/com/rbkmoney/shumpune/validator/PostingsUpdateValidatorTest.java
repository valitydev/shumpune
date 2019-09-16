package com.rbkmoney.shumpune.validator;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.base.InvalidRequest;
import com.rbkmoney.shumpune.domain.PostingModel;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PostingsUpdateValidatorTest {

    public static final long AMOUNT = 12L;
    public static final String RUB = "RUB";
    public static final long FROM_ID = 1L;
    public static final long TO_ID = 2L;
    public static final String DESCRIPTION = "test";
    PostingsUpdateValidator postingsUpdateValidator = new PostingsUpdateValidator();

    @Test(expected = InvalidRequest.class)
    public void validateBatchLowerThanSaved() throws TException {
        HashMap<Long, List<PostingModel>> savedBatches = new HashMap<>();
        long idBatch = 2L;
        savedBatches.put(idBatch, new ArrayList<>());
        PostingPlan receivedPostingPlan = new PostingPlan();
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        PostingBatch postingBatch = new PostingBatch();
        postingBatch.setId(1L);
        PostingBatch postingBatchSecond = new PostingBatch();
        postingBatchSecond.setId(idBatch);
        batchList.add(postingBatch);
        batchList.add(postingBatchSecond);
        receivedPostingPlan.setBatchList(batchList);
        postingsUpdateValidator.validate(receivedPostingPlan.getBatchList(), savedBatches);
    }

    @Test(expected = InvalidRequest.class)
    public void validateBatchSize() throws TException {
        HashMap<Long, List<PostingModel>> savedBatches = new HashMap<>();
        long idBatch = 2L;
        ArrayList<PostingModel> postingModels = new ArrayList<>();
        postingModels.add(new PostingModel());
        savedBatches.put(idBatch, postingModels);
        PostingPlan receivedPostingPlan = new PostingPlan();
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        PostingBatch postingBatch = new PostingBatch();
        postingBatch.setId(idBatch);
        batchList.add(postingBatch);
        receivedPostingPlan.setBatchList(batchList);
        postingsUpdateValidator.validate(receivedPostingPlan.getBatchList(), savedBatches);
    }

    @Test(expected = InvalidRequest.class)
    public void validateBatchPosting() throws TException {
        HashMap<Long, List<PostingModel>> savedBatches = new HashMap<>();
        long idBatch = 2L;
        ArrayList<PostingModel> postingModels = new ArrayList<>();
        postingModels.add(PostingModel.builder()
                .amount(AMOUNT)
                .currencySymbCode(RUB)
                .accountFromId(FROM_ID)
                .accountToId(TO_ID)
                .description(DESCRIPTION)
                .build());
        savedBatches.put(idBatch, postingModels);
        PostingPlan receivedPostingPlan = new PostingPlan();
        ArrayList<PostingBatch> batchList = new ArrayList<>();
        PostingBatch postingBatch = new PostingBatch();
        postingBatch.setId(idBatch);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setAmount(AMOUNT + 1)
                .setCurrencySymCode(RUB)
                .setFromId(FROM_ID)
                .setToId(TO_ID)
                .setDescription(DESCRIPTION));
        postingBatch.setPostings(postings);
        batchList.add(postingBatch);
        receivedPostingPlan.setBatchList(batchList);
        postingsUpdateValidator.validate(receivedPostingPlan.getBatchList(), savedBatches);
    }
}