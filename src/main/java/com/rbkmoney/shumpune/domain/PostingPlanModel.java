package com.rbkmoney.shumpune.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostingPlanModel {

    private PostingPlanInfo postingPlanInfo;
    private List<PostingModel> postingModels;

}
