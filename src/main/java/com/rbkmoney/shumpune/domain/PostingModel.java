package com.rbkmoney.shumpune.domain;

import com.rbkmoney.shumpune.constant.PostingOperation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostingModel {

    private String planId;
    private Long batchId;
    private Long accountFromId;
    private Long accountToId;
    private Long amount;
    private String currencySymbCode;
    private String description;
    private Instant creationTime;
    private PostingOperation operation;

}
