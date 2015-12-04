package com.egopulse.querydsl.mongodb;

import com.querydsl.core.QueryResults;
import rx.Observable;

import java.util.List;

public interface Fetchable<T> {
    Observable<List<T>> fetchFrom(String collection);

//    Observable<T> fetchFirst(String collection);

    Observable<T> fetchOneFrom(String collection);

//    QueryResults<T> fetchResults(String collection);

//    Observable<Long> fetchCount(String collection);
}
