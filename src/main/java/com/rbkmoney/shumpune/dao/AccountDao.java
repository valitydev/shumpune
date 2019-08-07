package com.rbkmoney.shumpune.dao;

import com.rbkmoney.damsel.shumpune.Account;
import com.rbkmoney.damsel.shumpune.AccountPrototype;
import com.rbkmoney.shumpune.domain.AccountSnapshot;

public interface AccountDao {

    Long insert(AccountPrototype account);

    AccountSnapshot getSnapshotById(Long id, Long clock);

}
