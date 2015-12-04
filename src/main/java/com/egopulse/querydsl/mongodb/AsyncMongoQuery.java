package com.egopulse.querydsl.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoCollection;
import com.mysema.commons.lang.CloseableIterator;
import com.querydsl.core.DefaultQueryMetadata;
import com.querydsl.core.QueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.SimpleQuery;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.*;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import rx.Observable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public abstract class AsyncMongoQuery<K, Q extends AsyncMongoQuery<K, Q>> implements SimpleQuery<Q>, Fetchable<K> {

    private final QueryMixin<Q> queryMixin;
    private final MongoCollection<Document> collection;
    private final MongodbSerializer serializer;

    public AsyncMongoQuery(MongoCollection<Document> collection) {
        @SuppressWarnings("unchecked")
        Q query = (Q) this;
        this.queryMixin = new QueryMixin<Q>(query, new DefaultQueryMetadata(), false);
        this.collection = collection;
        this.serializer = new MongodbSerializer();
    }

    @Override
    public Q distinct() {
        return queryMixin.distinct();
    }

    public Q where(Predicate e) {
        return queryMixin.where(e);
    }

    @Override
    public Q where(Predicate... e) {
        return queryMixin.where(e);
    }

    @Override
    public Q limit(long limit) {
        return queryMixin.limit(limit);
    }

    @Override
    public Q offset(long offset) {
        return queryMixin.offset(offset);
    }

    @Override
    public Q restrict(QueryModifiers modifiers) {
        return queryMixin.restrict(modifiers);
    }

    public Q orderBy(OrderSpecifier<?> o) {
        return queryMixin.orderBy(o);
    }

    @Override
    public Q orderBy(OrderSpecifier<?>... o) {
        return queryMixin.orderBy(o);
    }

    @Override
    public <T> Q set(ParamExpression<T> param, T value) {
        return queryMixin.set(param, value);
    }

    /**
     * Iterate with the specific fields
     *
     * @param paths fields to return
     * @return iterator
     */
    public CloseableIterator<Observable<K>> iterate(Path<?>... paths) {
        queryMixin.setProjection(paths);
        return iterate();
    }

    /**
     * Fetch with the specific fields
     *
     * @param paths fields to return
     * @return results
     */
    public Observable<List<K>> fetch(String collection, Path<?>... paths) {
        queryMixin.setProjection(paths);
        return fetch(collection);
    }

    @Override
    public Observable<List<K>> fetch() {
        MongodbSerializer serializer = new MongodbSerializer();

        QueryMetadata metadata = queryMixin.getMetadata();
        Predicate filterExpression = createFilter(metadata);


        Bson filterCondition = filterExpression == null ? new BsonDocument() : (Bson) serializer.handle(filterExpression);

        System.out.println(toJson(filterCondition));

        return Observable.create(subscriber -> {
            this.collection.find(filterCondition).into(new ArrayList<>(), (documents, throwable) -> {
                if (throwable != null) {
                    subscriber.onError(throwable);
                } else {
                    documents.stream().forEach((Document d) -> {
                        System.out.println(d.toJson(new JsonWriterSettings(true)));
                    });
                    subscriber.onNext(null);
                }

                subscriber.onCompleted();
            });
        });
    }

    protected String toJson(Bson bson) {
        return bson.toBsonDocument(Document.class, collection.getCodecRegistry()).toJson(new JsonWriterSettings(true));
    }

    public Observable<K> fetchOne(Path<?>... paths) {
        queryMixin.setProjection(paths);
        return fetchOne();
    }

    @Override
    public Observable<K> fetchOne() {
        return Observable.create(subscriber -> {
            this.collection.find(new Document()).first((documents, throwable) -> {
                if (throwable != null) {
                    subscriber.onError(throwable);
                } else {
                    System.out.println(documents);
                    subscriber.onNext(null);
                }

                subscriber.onCompleted();
            });
        });
    }

    private FindIterable<Document> rawFetch() {
        QueryMetadata metadata = queryMixin.getMetadata();
        Predicate filter = createFilter(metadata);

        BasicDBObject dbObject = createQuery(filter);
        Document document = new Document(dbObject);

        return this.collection.find(document);
    }

//    private DBObject createProjection(Expression<?> projection) {
//        if (projection instanceof FactoryExpression) {
//            DBObject obj = new BasicDBObject();
//            for (Object expr : ((FactoryExpression) projection).getArgs()) {
//                if (expr instanceof Expression) {
//                    obj.put((String) serializer.handle((Expression) expr), 1);
//                }
//            }
//            return obj;
//        }
//        return null;
//    }

    private BasicDBObject createQuery(@Nullable Predicate predicate) {

            return new BasicDBObject();
    }

    @SuppressWarnings("unchecked")
    @Nullable
    protected Predicate createJoinFilter(QueryMetadata metadata) {
//        Multimap<Expression<?>, Predicate> predicates = HashMultimap.create();
//        List<JoinExpression> joins = metadata.getJoins();
//        for (int i = joins.size() - 1; i >= 0; i--) {
//            JoinExpression join = joins.get(i);
//            Path<?> source = (Path) ((Operation<?>) join.getTarget()).getArg(0);
//            Path<?> target = (Path) ((Operation<?>) join.getTarget()).getArg(1);
//            Collection<Predicate> extraFilters = predicates.get(target.getRoot());
//            Predicate filter = ExpressionUtils.allOf(join.getCondition(), allOf(extraFilters));
//            List<? extends Object> ids = getIds(target.getType(), filter);
//            if (ids.isEmpty()) {
//                throw new NoResults();
//            }
//            Path<?> path = ExpressionUtils.path(String.class, source, "$id");
//            predicates.put(source.getRoot(), ExpressionUtils.in((Path<Object>) path, ids));
//        }
//        Path<?> source = (Path) ((Operation) joins.get(0).getTarget()).getArg(0);
//        return allOf(predicates.get(source.getRoot()));
        return null;
    }

    @Nullable
    protected Predicate createFilter(QueryMetadata metadata) {
        Predicate filter;
        if (!metadata.getJoins().isEmpty()) {
            filter = ExpressionUtils.allOf(metadata.getWhere(), createJoinFilter(metadata));
        } else {
            filter = metadata.getWhere();
        }
        return filter;
    }

}
