package com.rbkmoney.shumpune.converter;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.shumpune.domain.PostingModel;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class PostingModelToPostingBatchConverter implements Converter<List<PostingModel>, PostingBatch> {

    @Override
    public PostingBatch convert(List<PostingModel> postingModels) {
        return new PostingBatch()
                .setPostings(postingModels.stream()
                        .map(postingModel -> new Posting()
                                .setAmount(postingModel.getAmount())
                                .setCurrencySymCode(postingModel.getCurrencySymbCode())
                                .setFromId(postingModel.getAccountFromId())
                                .setToId(postingModel.getAccountToId())
                                .setDescription(postingModel.getDescription()))
                        .collect(Collectors.toList()));
    }

}
