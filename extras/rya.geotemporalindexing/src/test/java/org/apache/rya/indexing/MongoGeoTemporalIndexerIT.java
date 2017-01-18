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
package org.apache.rya.indexing;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.GeoTemporalIndexer.GeoPolicy;
import org.apache.rya.indexing.GeoTemporalIndexer.TemporalPolicy;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.model.Event;
import org.apache.rya.indexing.mongo.MongoGeoTemporalIndexer;
import org.apache.rya.mongodb.MockMongoFactory;
import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.mongodb.MongoClient;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import de.flapdoodle.embed.mongo.distribution.Version;
import info.aduna.iteration.CloseableIteration;

/**
 * JUnit tests for TemporalIndexer and it's implementation MongoTemporalIndexer
 *
 * If you enjoy this test, please read RyaTemporalIndexerTest and YagoKBTest, which contain
 * many example SPARQL queries and updates and attempts to test independently of Mongo:
 *
 *     extras/indexingSail/src/test/java/org.apache/rya/indexing/Mongo/RyaTemporalIndexerTest.java
 *     {@link org.apache.rya.indexing.Mongo.RyaTemporalIndexerTest}
 *     {@link org.apache.rya.indexing.Mongo.YagoKBTest.java}
 *
 * Remember, this class in instantiated fresh for each @test method.
 * so fields are reset, unless they are static.
 *
 * These are covered:
 *   Instance {before, equals, after} given Instance
 *   Instance {before, after, inside} given Interval
 *   Instance {hasBeginning, hasEnd} given Interval
 * And a few more.
 *
 */
public class MongoGeoTemporalIndexerIT extends GeoTemporalTestBase {
    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();
    private static final String URI_PROPERTY_AT_TIME = "Property:atTime";
    private static final TemporalInstant INSTANT = makeInstant(30);
    private static final TemporalInterval INTERVAL = new TemporalInterval(makeInstant(10), makeInstant(20));

    private static final Polygon A = poly(bbox(-3, -2, 1, 2));
    private static final Polygon B = poly(bbox(-3, -2, -1, 0));
    private static final Polygon C = poly(bbox(1, 0, 3, 2));
    private static final Polygon D = poly(bbox(0, -3, 2, -1));

    private static final Point F = point(-1, 1);
    private static final Point G = point(1, 1);

    private static final LineString E = line(-1, -3, 0, -1);

    public static MongoDBRdfConfiguration conf;
    public static MongoGeoTemporalIndexer indexer;

    protected static MockMongoFactory testsFactory;
    protected static MongoClient mongoClient;

    @Before
    public void setup() throws IOException {
        final Configuration conf = getConf();
        final List<RyaStatement> statements = getTestData();

        testsFactory = MockMongoFactory.with(Version.Main.PRODUCTION);
        mongoClient = testsFactory.newMongoClient();
        indexer = new MongoGeoTemporalIndexer();
        System.out.println(mongoClient.getAddress().toString());
        indexer.initIndexer(conf, mongoClient);
        indexer.storeStatements(statements);
    }

    @After
    public void MongoRyaTestBaseAfter() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (testsFactory != null) {
            testsFactory.shutdown();
        }
        MongoConnectorFactory.closeMongoClient();
    }

    public static Configuration getConf() {
        conf = new MongoDBRdfConfiguration();
        conf.set(ConfigUtils.USE_MONGO, "true");
        conf.setMongoDBName("test");
        conf.set(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya_");
        ((RdfCloudTripleStoreConfiguration) conf).setTablePrefix("isthisused_");

        // This is from http://linkedevents.org/ontology
        // and http://motools.sourceforge.net/event/event.html
        conf.setStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST, ""
                + URI_PROPERTY_AT_TIME);
        conf.set(ConfigUtils.GEO_PREDICATES_LIST, "http://www.opengis.net/ont/geosparql#asWKT");
        conf.set(OptionalConfigUtils.USE_GEO, "true");
        return conf;
    }

    private static List<RyaStatement> getTestData() {
        final List<RyaStatement> data = new ArrayList<>();
        final ValueFactory vf = new ValueFactoryImpl();
        final Resource subject = vf.createURI("foo:subj");
        final URI predicate = GeoConstants.GEO_AS_WKT;
        final Resource context = vf.createURI("foo:event0");

        /*
         *                     GEO STATEMENTS
         */
        Value object = vf.createLiteral(A.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        data.add(RdfToRyaConversions.convertStatement(new ContextStatementImpl(subject, predicate, object, context)));

        object = vf.createLiteral(B.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        data.add(RdfToRyaConversions.convertStatement(new ContextStatementImpl(subject, predicate, object, context)));

        object = vf.createLiteral(C.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        data.add(RdfToRyaConversions.convertStatement(new ContextStatementImpl(subject, predicate, object, context)));

        object = vf.createLiteral(D.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        data.add(RdfToRyaConversions.convertStatement(new ContextStatementImpl(subject, predicate, object, context)));

        object = vf.createLiteral(E.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        data.add(RdfToRyaConversions.convertStatement(new ContextStatementImpl(subject, predicate, object, context)));

        object = vf.createLiteral(F.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        data.add(RdfToRyaConversions.convertStatement(new ContextStatementImpl(subject, predicate, object, context)));

        object = vf.createLiteral(G.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        data.add(RdfToRyaConversions.convertStatement(new ContextStatementImpl(subject, predicate, object, context)));

        /*
         *                     TEMPORAL STATEMENTS
         */

        //Equals test interval
        final URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);
        TemporalInterval interval = new TemporalInterval(makeInstant(10), makeInstant(20));
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(interval.toString()))));

        //before test interval
        interval = new TemporalInterval(makeInstant(00), makeInstant(05));
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(interval.toString()))));

        //after test interval
        interval = new TemporalInterval(makeInstant(30), makeInstant(32));
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(interval.toString()))));

        //equals test instant
        TemporalInstant instant = makeInstant(30);
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(instant.getAsReadable()))));

        //before test instant
        instant = makeInstant(25);
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(instant.getAsReadable()))));

        //after test instant
        instant = makeInstant(35);
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(instant.getAsReadable()))));

        //before test interval
        instant = makeInstant(00);
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(instant.getAsReadable()))));

        //in test interval
        instant = makeInstant(15);
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(instant.getAsReadable()))));

        //after test interval
        instant = makeInstant(30);
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(instant.getAsReadable()))));

        //start of test interval
        instant = makeInstant(10);
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(instant.getAsReadable()))));

        //end of test interval
        instant = makeInstant(20);
        data.add(RdfToRyaConversions.convertStatement(new StatementImpl(subject, pred1_atTime, vf.createLiteral(instant.getAsReadable()))));

        return data;
    }

    private void test(final Event geoTime, final GeoPolicy gPolicy, final TemporalPolicy tPolicy, final Set<Statement> expected) throws Exception {
        final CloseableIteration<Statement, QueryEvaluationException> actual = indexer.query(geoTime, gPolicy, tPolicy, EMPTY_CONSTRAINTS);
        final Set<Statement> set = new HashSet<>();
        while (actual.hasNext()) {
            set.add(actual.next());
        }
        assertEquals(expected, set);
    }

    /*GEO POLICY IS EQUALS*/
    @Test
    public void testWithEquals_EqualsInstant() throws Exception {
        final Event geoTime = new Event(A, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithEquals_BeforeInstant() throws Exception {
        final Event geoTime = new Event(B, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithEquals_AfterInstant() throws Exception {
        final Event geoTime = new Event(C, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_AFTER_INSTANT, expected);
    }

    @Test
    public void testWithEquals_BeforeInterval() throws Exception {
        final Event geoTime = new Event(D, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_InInterval() throws Exception {
        final Event geoTime = new Event(E, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_IN_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_AfterInterval() throws Exception {
        final Event geoTime = new Event(F, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_StartInterval() throws Exception {
        final Event geoTime = new Event(G, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_START_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_EndInterval() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INSTANT_END_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_IntervalEquals() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithEquals_IntervalBefore() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithEquals_IntervalAfter() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS DISJOINT */
    @Test
    public void testWithDisjoint_EqualsInstant() throws Exception {
        final Event geoTime = new Event(A, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.INSTANT_EQUALS_INSTANT, expected);
    }

    /* GEO POLICY IS INTERSECTS */
    @Test
    public void testWithIntersects_EqualsInstant() throws Exception {
        final Event geoTime = new Event(A, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithIntersects_BeforeInstant() throws Exception {
        final Event geoTime = new Event(B, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithIntersects_AfterInstant() throws Exception {
        final Event geoTime = new Event(C, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_AFTER_INSTANT, expected);
    }

    @Test
    public void testWithIntersects_BeforeInterval() throws Exception {
        final Event geoTime = new Event(D, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_InInterval() throws Exception {
        final Event geoTime = new Event(E, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_IN_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_AfterInterval() throws Exception {
        final Event geoTime = new Event(F, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_StartInterval() throws Exception {
        final Event geoTime = new Event(G, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_START_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_EndInterval() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INSTANT_END_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_IntervalEquals() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithIntersects_IntervalBefore() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithIntersects_IntervalAfter() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS TOUCHES */
    @Test(expected=UnsupportedOperationException.class)
    public void testWithTouches_EqualsInstant() throws Exception {
        final Event geoTime = new Event(A, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.INSTANT_EQUALS_INSTANT, expected);
    }

    /* GEO POLICY IS CROSSES */
    @Test(expected=UnsupportedOperationException.class)
    public void testWithCrosses_EqualsInstant() throws Exception {
        final Event geoTime = new Event(A, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.INSTANT_EQUALS_INSTANT, expected);
    }

    /* GEO POLICY IS WITHIN */
    @Test
    public void testWithWithin_EqualsInstant() throws Exception {
        final Event geoTime = new Event(A, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithWithin_BeforeInstant() throws Exception {
        final Event geoTime = new Event(B, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithWithin_AfterInstant() throws Exception {
        final Event geoTime = new Event(C, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_AFTER_INSTANT, expected);
    }

    @Test
    public void testWithWithin_BeforeInterval() throws Exception {
        final Event geoTime = new Event(D, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_InInterval() throws Exception {
        final Event geoTime = new Event(E, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_IN_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_AfterInterval() throws Exception {
        final Event geoTime = new Event(F, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_StartInterval() throws Exception {
        final Event geoTime = new Event(G, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_START_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_EndInterval() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INSTANT_END_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_IntervalEquals() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithWithin_IntervalBefore() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithWithin_IntervalAfter() throws Exception {
        final Event geoTime = new Event(A, INTERVAL, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS CONTAINS */
    @Test(expected=UnsupportedOperationException.class)
    public void testWithContains_EqualsInstant() throws Exception {
        final Event geoTime = new Event(A, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.INSTANT_EQUALS_INSTANT, expected);
    }

    /* GEO POLICY IS OVERLAPS */
    @Test(expected=UnsupportedOperationException.class)
    public void testWithOverlaps_EqualsInstant() throws Exception {
        final Event geoTime = new Event(A, INSTANT, "");
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.INSTANT_EQUALS_INSTANT, expected);
    }

    private static List<StatementPattern> getSPs(final String sparql) throws MalformedQueryException {
        final StatementPatternCollector spCollector = new StatementPatternCollector();
        new SPARQLParser().parseQuery(sparql, null).getTupleExpr().visit(spCollector);
        return spCollector.getStatementPatterns();
    }
}
