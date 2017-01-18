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
package org.apache.rya.indexing.mongo;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoTemporalIndexer;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.model.Event;
import org.apache.rya.indexing.mongodb.AbstractMongoIndexer;
import org.apache.rya.indexing.mongodb.IndexingException;
import org.apache.rya.indexing.storage.EventStorage;
import org.apache.rya.indexing.storage.mongo.MongoEventStorage;
import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.joda.time.DateTime;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * TODO: doc
 */
public class MongoGeoTemporalIndexer extends AbstractMongoIndexer<GeoTemporalMongoDBStorageStrategy> implements GeoTemporalIndexer {
    private static final Logger LOG = Logger.getLogger(MongoGeoTemporalIndexer.class);
    public static final String GEO_TEMPORAL_COLLECTION = "geo_temporal";

    private final AtomicReference<MongoDBRdfConfiguration> configuration = new AtomicReference<>();
    private final AtomicReference<EventStorage> events = new AtomicReference<>();

    @Override
    public void init() {
        initCore();
        predicates = ConfigUtils.getGeoPredicates(conf);
        predicates.addAll(ConfigUtils.getTemporalPredicates(conf));
        storageStrategy = new GeoTemporalMongoDBStorageStrategy();
    }

    @Override
    public void setConf(final Configuration conf) {
        requireNonNull(conf);
        events.set(null);
        events.set(getEventStorage(conf));
        configuration.set(new MongoDBRdfConfiguration(conf));
    }

    @Override
    public void storeStatement(final RyaStatement ryaStatement) throws IOException {
        requireNonNull(ryaStatement);

        try {
            updateEvent(ryaStatement.getSubject(), ryaStatement);
        } catch (IndexingException | ParseException e) {
            throw new IOException("Failed to update the Entity index.", e);
        }
    }

    private void updateEvent(final RyaURI subject, final RyaStatement statement) throws IndexingException, ParseException {
        final EventStorage eventStore = events.get();
        checkState(events != null, "Must set this indexers configuration before storing statements.");

        new MongoGeoTemporalUpdater(eventStore).update(subject, old -> {
            final Event.Builder updated;
            if(!old.isPresent()) {
                updated = Event.builder()
                    .setSubject(subject);
            } else {
                updated = Event.builder(old.get());
            }

            final RyaURI pred = statement.getPredicate();
            if(pred.equals(GeoConstants.GEO_AS_WKT) || pred.equals(GeoConstants.GEO_AS_GML)) {
                //is geo
                try {

                    /*
                     * TODO: use the GML reader.
                     */

                final WKTReader reader = new WKTReader();
                final Geometry geometry = reader.read(statement.getObject().getData());
                updated.setGeometry(geometry);
                } catch (final ParseException e) {
                    LOG.error(e.getMessage(), e);
                }
            } else {
                //is time
                final String dateTime = statement.getObject().getData();
                final Matcher matcher = TemporalInstantRfc3339.PATTERN.matcher(dateTime);
                if (matcher.find()) {
                    final TemporalInterval interval = TemporalInstantRfc3339.parseInterval(dateTime);
                    updated.setTemporalInterval(interval);
                } else {
                    final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.parse(dateTime));
                    updated.setTemporalInstant(instant);
                }
            }
            return Optional.of(updated.build());
        });
    }

    @Override
    public String getCollectionName() {
        return ConfigUtils.getTablePrefix(conf)  + GEO_TEMPORAL_COLLECTION;
    }

    @Override
    public EventStorage getEventStorage(final Configuration conf) {
        if(events.get() != null) {
            return events.get();
        }

        final MongoDBRdfConfiguration mongoConf = (MongoDBRdfConfiguration) conf;
        mongoClient = mongoConf.getMongoClient();
        if (mongoClient == null) {
            mongoClient = MongoConnectorFactory.getMongoClient(conf);
        }
        final String ryaInstanceName = mongoConf.getMongoDBName();
        return new MongoEventStorage(mongoClient, ryaInstanceName);
    }
}
