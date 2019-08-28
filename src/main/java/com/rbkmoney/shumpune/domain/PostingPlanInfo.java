package com.rbkmoney.shumpune.domain;

import com.rbkmoney.shumpune.constant.PostingOperation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostingPlanInfo {

    private String id;
    private Long clock;
    private Long batchId;
    private PostingOperation postingOperation;

}
