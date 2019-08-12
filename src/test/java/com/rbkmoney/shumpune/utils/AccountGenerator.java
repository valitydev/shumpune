package com.rbkmoney.shumpune.utils;

import com.rbkmoney.damsel.shumpune.AccountPrototype;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public class AccountGenerator {

    @NotNull
    public static AccountPrototype createAccountPrototype(Instant now) {
        AccountPrototype accountPrototype = new AccountPrototype();
        accountPrototype.setCreationTime(now == null ? null : ISO_INSTANT.format(now));
        accountPrototype.setCurrencySymCode("RUB");
        accountPrototype.setDescription("test");
        return accountPrototype;
    }

}
