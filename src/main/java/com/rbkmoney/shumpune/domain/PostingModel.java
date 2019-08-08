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
public class PostingModel {

    public String planId;
    public Long batchId;
    public Long accountFromId;
    public Long accountToId;
    public Long amount;
    public String currencySymbCode;
    public String description;
    public PostingOperation operation;

}
