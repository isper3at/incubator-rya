/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.GeoTemporalIndexer.GeoPolicy;
import org.apache.rya.indexing.GeoTemporalIndexer.TemporalPolicy;
import org.apache.rya.indexing.model.Event;
import org.apache.rya.indexing.mongo.GeoTemporalMongoDBStorageStrategy;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.vividsolutions.jts.geom.Polygon;

public class GeoTemporalMongoDBStorageStrategyTest extends GeoTemporalTestBase {
    private static final TemporalInstant TEST_INSTANT = makeInstant(0);
    private static final TemporalInstant TEST_INTERVAL_START = makeInstant(0);
    private static final TemporalInstant TEST_INTERVAL_END = makeInstant(50);
    private static final TemporalInterval TEST_INTERVAL = new TemporalInterval(TEST_INTERVAL_START, TEST_INTERVAL_END);
    private static final Polygon POLYGON = poly(bbox(-3, -2, 1, 2));
    private static final RyaURI SUBJECT = new RyaURI("test:subject");

    private GeoTemporalMongoDBStorageStrategy adapter;
    @Before
    public void setup() {
        adapter = new GeoTemporalMongoDBStorageStrategy();
    }

    @Test
    public void withinInstantAfterInstant_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_AFTER_INSTANT);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"instant\" : { "
                    + "\"$gt\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                    + "}"
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void equalsInstantAfterInterval_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_AFTER_INTERVAL);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : "
                    + "[ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                + "} ,{ "
                + "  \"instant\" : { "
                    + "\"$gt\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:50.000Z\""
                    + "}"
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void intersectsInstantBeforeInstant_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_BEFORE_INSTANT);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoIntersects\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"instant\" : { "
                    + "\"$lt\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                    + "}"
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void withinInstantBeforeInterval_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_BEFORE_INTERVAL);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"instant\" : { "
                    + "\"$lt\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                    + "}"
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void withinInstantEndInterval_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_END_INTERVAL);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"instant\" : { "
                    + "\"$date\" : \"2015-12-30T12:00:50.000Z\""
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void withinInstantEqualsInstant_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_EQUALS_INSTANT);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"instant\" : { "
                    + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void withinInstantInInterval_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_IN_INTERVAL);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"instant\" : { "
                    + "\"$gt\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                    + "},"
                    + "\"$lt\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:50.000Z\""
                  + "}"
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void withinInstantStartInterval_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_START_INTERVAL);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"instant\" : { "
                    + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void withinIntervalAfter_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_AFTER);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"start\" : { "
                    + "\"$gt\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:50.000Z\""
                    + "}"
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void withinIntervalBefore_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_BEFORE);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"end\" : { "
                    + "\"$lt\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                    + "}"
                  + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void withinIntervalEquals_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        final DBObject actual = adapter.getQuery(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_EQUALS);
        final String expectedString =
                "{ "
                + "\"$or\" : [ { "
                  + "\"location\" : { "
                    + "\"$geoWithin\" : { "
                      + "\"$polygon\" : [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]"
                    + "}"
                  + "}"
                + "} ,{ "
                + "  \"start\" : { "
                      + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                  + "},"
                + "  \"end\" : { "
                  + "\"$date\" : \"2015-12-30T12:00:50.000Z\""
                + "}"
                + "}"
              + "]"
            + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    /*
     * PRECONDITION CHECKS
     */

    @Test(expected=GeoTemporalIndexException.class)
    public void preconditionInstant_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        adapter.getQuery(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.INTERVAL_AFTER);
    }

    @Test(expected=GeoTemporalIndexException.class)
    public void preconditionInterval_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INTERVAL, SUBJECT);
        adapter.getQuery(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.INSTANT_AFTER_INSTANT);
    }

    /*
     * UNSUPPORTED FUNCTIONS
     */
    @Test(expected=UnsupportedOperationException.class)
    public void unsupportedCONTAINS_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        adapter.getQuery(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.INSTANT_AFTER_INSTANT);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void unsupportedCROSSES_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        adapter.getQuery(geoTime, GeoPolicy.CROSSES, TemporalPolicy.INSTANT_AFTER_INSTANT);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void unsupportedDISJOINT_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        adapter.getQuery(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.INSTANT_AFTER_INSTANT);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void unsupportedOVERLAPS_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        adapter.getQuery(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.INSTANT_AFTER_INSTANT);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void unsupportedTOUCHES_test() throws Exception {
        final Event geoTime = new Event(POLYGON, TEST_INSTANT, SUBJECT);
        adapter.getQuery(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.INSTANT_AFTER_INSTANT);
    }
}
