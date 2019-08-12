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
                        .map(p -> PostingModel.builder()
                                .accountFromId(p.from_id)
                                .accountToId(p.to_id)
                                .amount(p.amount)
                                .batchId(batch.getId())
                                .planId(source.id)
                                .creationTime(Instant.now())
                                .operation(PostingOperation.HOLD)
                                .currencySymbCode(p.currency_sym_code)
                                .description(p.description)
                                .build()))
                .collect(Collectors.toList());
    }

}
