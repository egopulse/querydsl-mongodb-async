package com.egopulse.querydsl.mongodb;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriter;
import org.junit.Test;

import java.io.PrintWriter;

public class BsonQueryTest {

    @Test
    public void test() {
        JsonWriter writer = new JsonWriter(new PrintWriter(System.out));
        Bson eq = Filters.eq("com.value", "123");
        System.out.println(eq);
    }
}
