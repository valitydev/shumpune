package com.rbkmoney.shumpune.dao;

import com.rbkmoney.damsel.shumpune.AccountPrototype;
import com.rbkmoney.damsel.shumpune.Balance;
import com.rbkmoney.shumpune.domain.BalanceModel;

public interface AccountDao {

    Long insert(AccountPrototype account);

    BalanceModel getBalanceById(Long id, Long clock);

}
