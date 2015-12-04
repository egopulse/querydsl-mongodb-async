package com.egopulse.querydsl.mongodb;

import com.egopulse.querydsl.mongodb.domain.QUser;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoDatabase;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.BooleanExpression;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.junit.Test;

public class MongodbSerializerTest {

    private final MongodbSerializer serializer;
    private final MongoClient mongoClient;
    private final MongoDatabase mongoDatabase;
    private final CodecRegistry codecRegistry;

    BooleanExpression expression1 = QUser.user.firstName.eq("Random string");
    BooleanExpression expression2 = QUser.user.firstName.ne("Random string");
    BooleanExpression expression3 = QUser.user.age.gt(99);

    public MongodbSerializerTest() {
        this.mongoClient = MongoClients.create();
        this.serializer = new MongodbSerializer();
        this.mongoDatabase = mongoClient.getDatabase("test");
        this.codecRegistry = mongoDatabase.getCodecRegistry();
    }

    @Test
    public void simpleQuery() {
        System.out.println(toJsonSearchString(expression1));
        System.out.println(toJsonSearchString(expression2));
        System.out.println(toJsonSearchString(expression3));
    }

    @Test
    public void complicatedQuery() {
        BooleanExpression _expression1 = expression1.and(expression2.or(expression3));
        BooleanExpression _expression2 = expression1.or(expression2.or(expression3));

        System.out.println(toJsonSearchString(_expression1));
        System.out.println(toJsonSearchString(_expression2));

        System.out.println(toJsonSearchString(_expression1.and(_expression2).or(expression3).or(QUser.user.addresses.isEmpty())));
        System.out.println(toJsonSearchString(_expression1.and(_expression2).or(expression3).or(QUser.user.addresses.isNotEmpty())));

        System.out.println(toJsonSearchString(expression1.and(expression2.or(expression3))));
        System.out.println(toJsonSearchString(expression1.and(expression2.or(expression3))));
    }

    protected String toJsonSearchString(Expression<?> expression) {
        Bson bson = (Bson) this.serializer.handle(expression);
        return bson.toBsonDocument(Document.class, mongoDatabase.getCodecRegistry()).toJson(new JsonWriterSettings(true));
    }

}
