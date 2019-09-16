package com.rbkmoney.shumpune.validator;

import com.rbkmoney.damsel.shumpune.Account;
import com.rbkmoney.damsel.shumpune.InvalidPostingParams;
import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.base.InvalidRequest;
import com.rbkmoney.shumpune.dao.AccountDao;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Optional;

public class PostingBatchValidatorTest {

    private static final String PLAN_ID = "plan_id";
    private static final String RUB = "RUB";

    @Mock
    private AccountDao accountDao;

    private PostingBatchValidator postingBatchValidator;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        postingBatchValidator = new PostingBatchValidator(accountDao);
    }

    @Test(expected = InvalidRequest.class)
    public void validateNoBatch() throws TException {
        postingBatchValidator.validate(null, PLAN_ID);
    }

    @Test(expected = InvalidRequest.class)
    public void validateEmptyBatch() throws TException {
        postingBatchValidator.validate(new PostingBatch(), PLAN_ID);
    }

    @Test(expected = InvalidPostingParams.class)
    public void validateEqualsSourceAndNotFoundAccount() throws TException {
        PostingBatch batch = new PostingBatch();
        batch.setId(1L);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting());
        batch.setPostings(postings);
        postingBatchValidator.validate(batch, PLAN_ID);
    }

    @Test(expected = InvalidPostingParams.class)
    public void validateNegativeAmount() throws TException {
        PostingBatch batch = new PostingBatch();
        batch.setId(1L);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setAmount(-100L)
                .setToId(1L)
                .setFromId(2L)
                .setCurrencySymCode(RUB));
        batch.setPostings(postings);

        Mockito.when(accountDao.getAccountById(1L)).thenReturn(createAccount(1L));
        Mockito.when(accountDao.getAccountById(2L)).thenReturn(createAccount(2L));

        postingBatchValidator.validate(batch, PLAN_ID);
    }

    @NotNull
    private Optional<Account> createAccount(long l) {
        return Optional.of(new Account()
                .setId(l)
                .setCurrencySymCode(RUB));
    }

    @Test(expected = InvalidPostingParams.class)
    public void validateNotEqualsCurrencies() throws TException {
        PostingBatch batch = new PostingBatch();
        batch.setId(1L);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setAmount(100L)
                .setToId(1L)
                .setFromId(2L)
                .setCurrencySymCode("USD"));
        batch.setPostings(postings);

        Mockito.when(accountDao.getAccountById(1L)).thenReturn(createAccount(1L));
        Mockito.when(accountDao.getAccountById(2L)).thenReturn(createAccount(2L));

        postingBatchValidator.validate(batch, PLAN_ID);
    }

    @Test
    public void validateTrue() throws TException {
        PostingBatch batch = new PostingBatch();
        batch.setId(1L);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setAmount(100L)
                .setToId(1L)
                .setFromId(2L)
                .setCurrencySymCode(RUB));
        batch.setPostings(postings);

        Mockito.when(accountDao.getAccountById(1L)).thenReturn(createAccount(1L));
        Mockito.when(accountDao.getAccountById(2L)).thenReturn(createAccount(2L));

        postingBatchValidator.validate(batch, PLAN_ID);
    }
}