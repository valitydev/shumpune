package com.rbkmoney.shumpune.converter;

import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.domain.PostingPlanInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PostingPlanToPostingPlanInfoConverter implements Converter<PostingPlan, PostingPlanInfo> {

    @Override
    public PostingPlanInfo convert(PostingPlan source) {
        return PostingPlanInfo.builder()
                .id(source.id)
                .batchId(source.getBatchList().stream()
                        .mapToLong(PostingBatch::getId)
                        .max().getAsLong())
                .postingOperation(PostingOperation.HOLD)
                .build();
    }

}
