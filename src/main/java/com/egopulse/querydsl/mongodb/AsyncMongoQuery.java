package com.egopulse.querydsl.mongodb;

import com.mongodb.rx.client.MongoDatabase;
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
import rx.Observable;

import javax.annotation.Nullable;
import java.util.List;

public abstract class AsyncMongoQuery<K, Q extends AsyncMongoQuery<K, Q>> implements SimpleQuery<Q>, Fetchable<K> {

    private final QueryMixin<Q> queryMixin;
    private final MongodbSerializer serializer;
    private final MongoDatabase database;

    protected AsyncMongoQuery(MongoDatabase database) {
        this.database = database;
        @SuppressWarnings("unchecked")
        Q query = (Q) this;
        this.queryMixin = new QueryMixin<Q>(query, new DefaultQueryMetadata(), false);
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
    public Observable<List<K>> fetchFrom(String collection, Path<?>... paths) {
        queryMixin.setProjection(paths);
        return fetchFrom(collection);
    }

    @Override
    public Observable<List<K>> fetchFrom(String collection) {
        QueryMetadata metadata = queryMixin.getMetadata();

        Predicate filterExpression = createFilter(metadata);
        Bson filterCondition = filterExpression == null ? new BsonDocument() : (Bson) serializer.handle(filterExpression);
        Bson projection = createProjection(metadata.getProjection());
        Bson sort = createSort(metadata.getOrderBy());

        return this.database.getCollection(collection)
                .find(filterCondition)
                .projection(projection)
                .sort(sort)
                // TODO conversion here
                .toObservable().map(doc -> {
                    System.out.println(doc.toJson());
                    return null;
                });
    }

    public Observable<K> fetchOneFrom(String collection, Path<?>... paths) {
        queryMixin.setProjection(paths);
        return fetchOneFrom(collection);
    }

    @Override
    public Observable<K> fetchOneFrom(String collection) {
        return this.database.getCollection(collection).find(new Document()).first().map(d -> null);
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

    private Bson createSort(List<OrderSpecifier<?>> orderBys) {
        return serializer.toSort(orderBys);
    }

    private Bson createProjection(Expression<?> projection) {
        if (projection instanceof FactoryExpression) {
            Document obj = new Document();
            for (Object expr : ((FactoryExpression) projection).getArgs()) {
                if (expr instanceof Expression) {
                    obj.put((String) serializer.handle((Expression) expr), 1);
                }
            }
            return obj;
        }
        return null;
    }

    private Predicate createFilter(QueryMetadata metadata) {
        Predicate filter;
        if (!metadata.getJoins().isEmpty()) {
            filter = ExpressionUtils.allOf(metadata.getWhere(), createJoinFilter(metadata));
        } else {
            filter = metadata.getWhere();
        }
        return filter;
    }

}
