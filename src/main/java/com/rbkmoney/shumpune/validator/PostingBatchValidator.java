package com.rbkmoney.shumpune.validator;


import com.rbkmoney.damsel.shumpune.InvalidPostingParams;
import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.*;

@Slf4j
@Component
public class PostingBatchValidator {

    public static final String SOURCE_TARGET_ACC_EQUAL_ERR = "Source and target accounts cannot be the same";
    public static final String AMOUNT_NEGATIVE_ERR = "Amount cannot be negative";

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
