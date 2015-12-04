package com.egopulse.querydsl.mongodb;

import com.egopulse.querydsl.mongodb.domain.User;
import com.mongodb.async.client.MongoCollection;
import org.bson.Document;

public class UserMongoQuery extends AsyncMongoQuery<User, UserMongoQuery> {

    public UserMongoQuery(MongoCollection<Document> collection) {
        super(collection);
    }

}
