package com.rbkmoney.shumpune.validator;


import com.rbkmoney.damsel.shumpune.Account;
import com.rbkmoney.damsel.shumpune.InvalidPostingParams;
import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.shumpune.dao.AccountDao;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class PostingBatchValidator {

    private static final String SOURCE_TARGET_ACC_EQUAL_ERR = "Source and target accounts cannot be the same";
    private static final String AMOUNT_NEGATIVE_ERR = "Amount cannot be negative";
    private static final String ACCOUNT_FOR_POSTING_NOT_FOUND = "Account for posting not found!";
    private static final String CURRENCY_ACCOUNT_NOT_EQUALS_OPERATION_CURRENCY = "Currency account not equals OPERATION currency!";

    private final AccountDao accountDao;

    public void validate(PostingBatch batch) throws TException {
        Map<Posting, String> errors = new HashMap<>();
        for (Posting posting : batch.postings) {
            List<String> errorMessages = new ArrayList<>();
            if (posting.getFromId() == posting.getToId()) {
                errorMessages.add(SOURCE_TARGET_ACC_EQUAL_ERR);
            }
            if (posting.getAmount() < 0) {
                errorMessages.add(AMOUNT_NEGATIVE_ERR);
            }

            Optional<Account> fromAccount = accountDao.getAccountById(posting.getFromId());
            Optional<Account> toAccount = accountDao.getAccountById(posting.getToId());
            String currencySymCode = posting.getCurrencySymCode();

            if (fromAccount.isEmpty() || toAccount.isEmpty()) {
                errorMessages.add(ACCOUNT_FOR_POSTING_NOT_FOUND);
            }

            if ((fromAccount.isPresent() && !fromAccount.get().getCurrencySymCode().equals(currencySymCode))
                    || (toAccount.isPresent() && !toAccount.get().getCurrencySymCode().equals(currencySymCode))) {
                errorMessages.add(CURRENCY_ACCOUNT_NOT_EQUALS_OPERATION_CURRENCY);
            }

            if (!errorMessages.isEmpty()) {
                errors.put(posting, generateMessage(errorMessages));
            }
        }
        if (!errors.isEmpty()) {
            throw new InvalidPostingParams(errors);
        }
    }

    private String generateMessage(Collection<String> msgs) {
        return StringUtils.collectionToDelimitedString(msgs, "; ");
    }

}
