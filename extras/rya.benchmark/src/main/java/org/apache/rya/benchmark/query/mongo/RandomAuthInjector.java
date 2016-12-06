/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.benchmark.query.mongo;

import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.rya.benchmark.query.QueriesBenchmarkConf;
import org.apache.rya.benchmark.query.QueriesBenchmarkConfReader;
import org.apache.rya.benchmark.query.Rya;
import org.apache.rya.benchmark.query.Rya.Mongo;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class RandomAuthInjector {
    /**
     * The path to the configuration file that this benchmark uses to connect to Rya.
     */
    public static final Path QUERY_BENCHMARK_CONFIGURATION_FILE = Paths.get("queries-benchmark-conf.xml");
    private static final String BASE_URI = "https://example.org/example/local";

    public static void main(final String [] args) throws Exception {
        // Load the benchmark's configuration file.
        final InputStream queriesStream = Files.newInputStream(QUERY_BENCHMARK_CONFIGURATION_FILE);
        final List<File> files = new ArrayList<>();
        for(int ii = 0; ii < args.length; ii++) {
            files.add(Paths.get(args[ii]).toFile());
        }
        final QueriesBenchmarkConf benchmarkConf = new QueriesBenchmarkConfReader().load(queriesStream);

        // Create the Rya Configuration object using the benchmark's configuration.
        final MongoDBRdfConfiguration ryaConf = new MongoDBRdfConfiguration();

        final Rya rya = benchmarkConf.getRya();
        ryaConf.setTablePrefix(rya.getRyaInstanceName());

        final Mongo confMongo = rya.getMongo();
        final String usr = confMongo.getUsername();
        if(usr != null) {
            ryaConf.set(MongoDBRdfConfiguration.MONGO_USER, usr);
        }
        final String pwd = confMongo.getPassword();
        if(pwd != null) {
            ryaConf.set(MongoDBRdfConfiguration.MONGO_USER_PASSWORD, pwd);
        }
        ryaConf.setMongoInstance(confMongo.getHostname());
        ryaConf.setMongoPort(confMongo.getPort());
        ryaConf.setMongoDBName(confMongo.getDatabase());

        // Print the query plan so that you can visually inspect how PCJs are being applied for each benchmark.
        ryaConf.set(ConfigUtils.DISPLAY_QUERY_PLAN, "true");

        // Turn off PCJs since they are unsupported in mongo.
        ryaConf.set(ConfigUtils.USE_PCJ, "false");

        ryaConf.setBoolean(ConfigUtils.USE_MONGO, true);

        final MongoClient client = new MongoClient(confMongo.getHostname(), Integer.parseInt(confMongo.getPort()));
        final MongoDatabase db = client.getDatabase(confMongo.getDatabase());
        final MongoCollection coll = db.getCollection("rya_triples_order");
        final Iterator<Document> docs = coll.find().iterator();
        System.out.println("Updating docs");
        while(docs.hasNext()) {
            final List<String> auths = generateNextAuths();
            final Document doc = docs.next();
            final BasicDBObject orig = BasicDBObject.parse(doc.toJson());
            if(doc.containsKey(DOCUMENT_VISIBILITY)) {
                doc.replace(DOCUMENT_VISIBILITY, auths);
            } else {
                doc.put(DOCUMENT_VISIBILITY, auths);
            }
            final BasicDBObject update = BasicDBObject.parse(doc.toJson());
            coll.replaceOne(orig, update);
        }
        client.close();
    }

    private static final List<List<String>> usedAuths = new ArrayList<>();
    private static List<String> generateNextAuths() {
        final List<Boolean> used = new ArrayList<>(Collections.nCopies(15, false));
        final List<String> auths = new ArrayList<>();
        for(int ii = 0; ii < 15; ii++) {
            int car = (int) Math.round(Math.random() * 14);
            while(used.get(car)) {
                car = (int) Math.round(Math.random() * 14);
            }
            used.set(car, true);
            auths.add("" + (char)(65+car));
        }
        if(usedAuths.contains(auths)) {
            return generateNextAuths();
        } else {
            usedAuths.add(auths);
            return auths;
        }
    }
}
