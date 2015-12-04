package com.egopulse.querydsl.mongodb;

import com.mongodb.rx.client.MongoDatabase;
import org.bson.Document;

public class MongoQuery extends AsyncMongoQuery<Document, MongoQuery> {

    protected MongoQuery(MongoDatabase database) {
        super(database);
    }

    public static MongoQuery forDatabase(MongoDatabase database) {
        return new MongoQuery(database);
    }

}
