package com.rbkmoney.shumpune.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AccountLogFields {

    public static final String ID = "id";
    public static final String ACCOUNT_ID = "account_id";
    public static final String OWN_AMOUNT = "own_amount";
    public static final String MAX_AVAILABLE_AMOUNT = "max_available_amount";
    public static final String MIN_AVAILABLE_AMOUNT = "min_available_amount";
    public static final String CREATION_TIME = "creation_time";
    public static final String DESCRIPTION = "description";
    public static final String CLOCK = "clock";

}