package com.rbkmoney.shumpune.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountSnapshot {

    public Long id;
    public Long ownAmount;
    public Long maxAvailableAmount;
    public Long minAvailableAmount;
    public String currencySymCode;
    public String description;
    public Instant creationTime;
    public Long clock;

}
