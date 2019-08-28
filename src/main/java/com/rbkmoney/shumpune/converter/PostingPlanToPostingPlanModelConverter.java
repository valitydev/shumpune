package com.rbkmoney.shumpune.converter;

import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.domain.PostingPlanInfo;
import com.rbkmoney.shumpune.domain.PostingPlanModel;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PostingPlanToPostingPlanModelConverter {

    private final PostingPlanChangeToListPostingModelConverter planToListPostingModelConverter;

    public PostingPlanModel convert(PostingPlanChange source, PostingOperation operation) {
        return PostingPlanModel.builder()
                .postingPlanInfo(PostingPlanInfo.builder()
                        .id(source.id)
                        .batchId(source.batch.getId())
                        .postingOperation(operation)
                        .build())
                .postingModels(planToListPostingModelConverter.convert(source, operation))
                .build();
    }

}
