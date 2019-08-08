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

    public Long id;
    public Long ownAmount;
    public Long maxAvailableAmount;
    public Long minAvailableAmount;
    public Long clock;

}
