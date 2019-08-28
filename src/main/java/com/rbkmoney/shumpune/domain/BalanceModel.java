package com.rbkmoney.shumpune.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BalanceModel {

    private Long accountId;
    private Long ownAmount;
    private Long maxAvailableAmount;
    private Long minAvailableAmount;
    private Long clock;

}
