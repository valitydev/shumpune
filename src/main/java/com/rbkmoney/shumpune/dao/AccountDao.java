package com.rbkmoney.shumpune.dao;

import com.rbkmoney.damsel.shumpune.Account;
import com.rbkmoney.damsel.shumpune.AccountPrototype;
import com.rbkmoney.shumpune.domain.BalanceModel;

import java.util.Optional;

public interface AccountDao {

    Long insert(AccountPrototype account);

    Optional<Account> getAccountById(Long id);
}
