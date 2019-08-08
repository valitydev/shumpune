package com.rbkmoney.shumpune.converter;

import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.domain.PostingPlanInfo;
import com.rbkmoney.shumpune.domain.PostingPlanModel;
import lombok.RequiredArgsConstructor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PostingPlanToPostingPlanModelConverter implements Converter<PostingPlanChange, PostingPlanModel> {

    private final PostingPlanToListPostingModelConverter planToListPostingModelConverter;

    @Override
    public PostingPlanModel convert(PostingPlanChange source) {
        return PostingPlanModel.builder()
                .postingPlanInfo(PostingPlanInfo.builder()
                        .id(source.id)
                        .batchId(source.batch.getId())
                        .postingOperation(PostingOperation.HOLD)
                        .build())
                .postingModels(planToListPostingModelConverter.convert(source))
                .build();
    }

}
