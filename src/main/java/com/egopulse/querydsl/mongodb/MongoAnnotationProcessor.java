package com.egopulse.querydsl.mongodb;

import com.querydsl.apt.AbstractQuerydslProcessor;
import com.querydsl.apt.Configuration;
import com.querydsl.apt.DefaultConfiguration;
import com.querydsl.core.annotations.QueryEntities;
import com.querydsl.core.annotations.QuerySupertype;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Transient;

import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import java.util.Collections;

@SupportedAnnotationTypes({"com.querydsl.core.annotations.*", "org.mongodb.morphia.annotations.*", "com.egopulse.querydsl.mongodb.*"})
public class MongoAnnotationProcessor extends AbstractQuerydslProcessor {

    public MongoAnnotationProcessor() {
    }

    protected Configuration createConfiguration(RoundEnvironment roundEnv) {
        Class entities = QueryEntities.class;
        Class entity = Entity.class;
        Class superType = QuerySupertype.class;
        Class embedded = Embedded.class;
        Class skip = Transient.class;
        DefaultConfiguration conf = new DefaultConfiguration(roundEnv, this.processingEnv.getOptions(), Collections.emptySet(), entities, entity, superType, (Class)null, embedded, skip);

        try {
            Class e = Class.forName("com.egopulse.querydsl.mongodb.Point");
            conf.addCustomType(Double[].class, e);
            return conf;
        } catch (ClassNotFoundException var9) {
            throw new IllegalStateException(var9);
        }
    }

}
