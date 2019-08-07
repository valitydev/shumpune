package com.rbkmoney.shumpune.dao;

import com.rbkmoney.damsel.shumpune.Account;
import com.rbkmoney.damsel.shumpune.AccountPrototype;

public interface AccountDao {

    Long insert(AccountPrototype account);

    Account getSnapshotById(Long id, Long clock);

}
