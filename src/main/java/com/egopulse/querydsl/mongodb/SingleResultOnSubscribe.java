package com.egopulse.querydsl.mongodb;

import com.mongodb.async.SingleResultCallback;
import rx.Observable;
import rx.Subscriber;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

final class SingleResultCallable<T> implements Callable<T>, SingleResultCallback<T> {

    private AtomicBoolean finished = new AtomicBoolean(false);
    private T t;
    private Throwable throwable;

    @Override
    public void onResult(T t, Throwable throwable) {
        this.t = t;
        this.throwable = throwable;
        finished.set(true);
    }

    @Override
    public T call() throws Exception {
        return null;
    }
}