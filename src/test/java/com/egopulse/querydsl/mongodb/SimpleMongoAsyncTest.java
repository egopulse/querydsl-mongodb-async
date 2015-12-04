package com.egopulse.querydsl.mongodb;

import com.egopulse.querydsl.mongodb.domain.User;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.mongodb.DBObject;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.egopulse.querydsl.mongodb.domain.QUser;
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

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleMongoAsyncTest {

    private final IMongodConfig config;

    private final MongodExecutable exe;
    private final MongodProcess mongoProcess;

    private MongoClient client;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    private Morphia morphia;

    public SimpleMongoAsyncTest() throws IOException {
        // Start embeded mongo
        config = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(27018, Network.localhostIsIPv6()))
                .build();
        exe = MongodStarter.getDefaultInstance().prepare(config);
        mongoProcess = exe.start();

        AtomicBoolean started = new AtomicBoolean(false);

        new Timer().schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        started.set(true);
                    }
        }, 2000);

        Awaitility.await().untilTrue(started);

        // Start mongo client
        client = MongoClients.create("mongodb://localhost:27018");
        database = client.getDatabase("test");
        collection = database.getCollection("user");

        morphia = new Morphia().mapPackage("com.querydsl.mongodb.domain");
    }

    @After
    public void cleanSh1t() {
        // Delete every single thing
        collection.deleteMany(new BsonDocument(), (deleteResult, throwable) -> {
            if (throwable != null) {
                System.err.println(throwable.getMessage());
            }
        });
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

        UserMongoQuery query = new UserMongoQuery(collection);

        query.fetch().subscribe(__ -> {
            System.out.println("I am here");
        });

        query.where(QUser.user.firstName.startsWith("J").and(QUser.user.lastName.startsWith("J")))
                .fetch()
                .subscribe(users -> {
                    finished.set(true);
                });

        Awaitility.await().untilTrue(finished);
    }

    private Observable<User> addUser(String first, String last) {
        User user = new User(first, last);

        DBObject dbObject = morphia.getMapper().toDBObject(user);
        Document document = new Document(dbObject.toMap());

        return Observable.create(subscriber -> {
            collection.insertOne(document, (__, throwable) -> {
                if (throwable != null) {
                    subscriber.onError(throwable);
                } else {
                    subscriber.onNext(user);
                }

                subscriber.onCompleted();
            });
        });

    }

}
