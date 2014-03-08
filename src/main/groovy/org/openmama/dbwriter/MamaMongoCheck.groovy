#!/usr/bin/env groovy -cp /Users/ppreston/.m2/repository/org/mongodb/mongo-java-driver/2.11.0/mongo-java-driver-2.11.0.jar
package org.openmama.dbwriter

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.ServerAddress

def DATABASE_NAME = 'openmama'
def COLLECTION_NAME = 'tickstore'

def client = new MongoClient(new ServerAddress('127.0.0.1'));
def db = client.getDB(DATABASE_NAME);
def tickstore_c = db.getCollection(COLLECTION_NAME)

def data_query = ['_id': 0, 'symbol': 1, 'seq_num': 1] as BasicDBObject;
def sort_query = ['symbol': 1, 'seq_num': 1] as BasicDBObject;
def cursor = tickstore_c.find(new BasicDBObject(), data_query).sort(sort_query);

cursor.each {
    item ->
        println "${item}"
}
