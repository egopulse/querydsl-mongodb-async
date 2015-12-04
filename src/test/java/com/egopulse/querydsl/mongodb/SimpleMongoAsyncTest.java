package com.egopulse.querydsl.mongodb;

import com.egopulse.querydsl.mongodb.domain.User;
import com.jayway.awaitility.Awaitility;
import com.mongodb.DBObject;
import com.egopulse.querydsl.mongodb.domain.QUser;
import com.mongodb.rx.client.MongoClient;
import com.mongodb.rx.client.MongoClients;
import com.mongodb.rx.client.MongoCollection;
import com.mongodb.rx.client.MongoDatabase;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Morphia;
import rx.Observable;
import rx.Single;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleMongoAsyncTest {

    private final IMongodConfig config;

    private final MongodExecutable exe;
    private final MongodProcess mongoProcess;

    private MongoClient client;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    private Morphia morphia;

    private static final String COLLECTION_NAME = "user";

    public SimpleMongoAsyncTest() throws IOException {
        // Start embeded mongo
        config = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(27018, Network.localhostIsIPv6()))
                .build();
        exe = MongodStarter.getDefaultInstance().prepare(config);
        mongoProcess = exe.start();

        AtomicBoolean started = new AtomicBoolean(false);

        // Start mongo client
        client = MongoClients.create("mongodb://localhost:27018");
        database = client.getDatabase("test");
        collection = database.getCollection(COLLECTION_NAME);

        morphia = new Morphia().mapPackage("com.querydsl.mongodb.domain");
    }

    @After
    public void cleanSh1t() {
        AtomicBoolean finished = new AtomicBoolean(false);
        // Delete every single thing
        collection.deleteMany(new BsonDocument()).subscribe(__ -> {
            finished.set(true);
        });
        Awaitility.await().untilTrue(finished);
    }

    @Before
    public void setup() {
        AtomicBoolean finished = new AtomicBoolean(false);

        Observable.zip(
                addUser("Jaakko", "Jantunen"),
                addUser("Jaakki", "Jantunen"),
                addUser("Jaana", "Aakkonen"),
                addUser("Jaana", "BeekkoNen"),
                (user1, user2, user3, user4) -> {
                    finished.set(true);
                    return null;
                }
        ).subscribe();

        Awaitility.await().untilTrue(finished);
    }

    @Test
    public void simpleTest() {
        AtomicBoolean finished = new AtomicBoolean(false);

        MongoQuery query = MongoQuery.forDatabase(database);

        query.fetchFrom(COLLECTION_NAME).subscribe(__ -> {
            System.out.println("I am here");
        });

        query.where(QUser.user.firstName.startsWith("J").and(QUser.user.lastName.startsWith("J")))
                .fetchFrom(COLLECTION_NAME, QUser.user.firstName, QUser.user.lastName)
                .subscribe(users -> {
                    finished.set(true);
                });

        Awaitility.await().untilTrue(finished);
    }

    private Observable<User> addUser(String first, String last) {
        User user = new User(first, last);

        DBObject dbObject = morphia.getMapper().toDBObject(user);
        Document document = new Document(dbObject.toMap());

        return collection.insertOne(document)
                .map(__ -> user)
                .toSingle()
                .toObservable();
    }

}
