package com.rbkmoney.shumpune.utils;

import com.rbkmoney.damsel.shumpune.AccountPrototype;
import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.ArrayList;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public class PostingGenerator {

    @NotNull
    public static PostingBatch createBatch(Long providerAcc, Long systemAcc, Long merchantAcc) {
        PostingBatch batch = new PostingBatch();
        batch.setId(1L);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(300000)
                .setFromId(providerAcc)
                .setToId(merchantAcc)
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(9000)
                .setFromId(merchantAcc)
                .setToId(systemAcc)
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(6000)
                .setFromId(systemAcc)
                .setToId(providerAcc)
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }


    @NotNull
    public static PostingPlanChange createPostingPlanChange(String planId, Long providerAcc, Long systemAcc, Long merchantAcc) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc);
        postingPlanChange.setBatch(batch)
                .setId(planId);
        return postingPlanChange;
    }
}
