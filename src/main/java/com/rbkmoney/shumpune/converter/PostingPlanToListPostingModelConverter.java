package com.rbkmoney.shumpune.converter;

import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.domain.PostingModel;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class PostingPlanToListPostingModelConverter implements Converter<PostingPlanChange, List<PostingModel>> {

    @Override
    public List<PostingModel> convert(PostingPlanChange source) {
        return source.getBatch().postings.stream()
                .map(p -> PostingModel.builder()
                        .accountFromId(p.from_id)
                        .accountToId(p.to_id)
                        .amount(p.amount)
                        .batchId(source.getBatch().getId())
                        .planId(source.id)
                        .operation(PostingOperation.HOLD)
                        .currencySymbCode(p.currency_sym_code)
                        .description(p.description)
                        .build())
                .collect(Collectors.toList());
    }

}
