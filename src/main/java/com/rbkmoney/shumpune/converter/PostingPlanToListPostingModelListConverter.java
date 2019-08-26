package com.rbkmoney.shumpune.converter;

import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.domain.PostingModel;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class PostingPlanToListPostingModelListConverter implements Converter<PostingPlan, List<PostingModel>> {

    @Override
    public List<PostingModel> convert(PostingPlan source) {
        return source.getBatchList()
                .stream()
                .flatMap(batch -> batch.getPostings().stream()
                        .map(posting -> PostingModel.builder()
                                .accountFromId(posting.from_id)
                                .accountToId(posting.to_id)
                                .amount(posting.amount)
                                .batchId(batch.getId())
                                .planId(source.id)
                                .creationTime(Instant.now())
                                .operation(PostingOperation.HOLD)
                                .currencySymbCode(posting.currency_sym_code)
                                .description(posting.description)
                                .build()))
                .collect(Collectors.toList());
    }

}
