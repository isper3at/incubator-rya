package org.apache.rya.indexing;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.GeoTemporalIndexer.GeoPolicy;
import org.apache.rya.indexing.GeoTemporalIndexer.TemporalPolicy;
import org.apache.rya.indexing.model.GeoTime;
import org.apache.rya.mongodb.MongoRyaTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

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
@RunWith(value = Parameterized.class)
public abstract class GeoTemporalIndexerTest extends MongoRyaTestBase {
    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();
    private static final String URI_PROPERTY_AT_TIME = "Property:atTime";
    private static final TemporalInstant INSTANT = makeInstant(30);
    private static final TemporalInterval INTERVAL = new TemporalInterval(makeInstant(10), makeInstant(20));

    private static final GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
    private static final Polygon A = poly(bbox(-3, -2, 1, 2));
    private static final Polygon B = poly(bbox(-3, -2, -1, 0));
    private static final Polygon C = poly(bbox(1, 0, 3, 2));
    private static final Polygon D = poly(bbox(0, -3, 2, -1));

    private static final Point F = point(-1, 1);
    private static final Point G = point(1, 1);

    private static final LineString E = line(-1, -3, 0, -1);

    @Parameter
    public Configuration conf;
    @Parameter(value = 1)
    public GeoTemporalIndexer indexer;

    public static Configuration getConf() {
        // TODO Auto-generated method stub
        return null;
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

    /**
     * Make an uniform instant with given seconds.
     */
    private static TemporalInstant makeInstant(final int secondsMakeMeUnique) {
        return new TemporalInstantRfc3339(2015, 12, 30, 12, 00, secondsMakeMeUnique);
    }

    private static Polygon poly(final double[] arr) {
        final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(arr, 2));
        final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
        return p1;
    }

    private static Point point(final double x, final double y) {
        return gf.createPoint(new Coordinate(x, y));
    }

    private static LineString line(final double x1, final double y1, final double x2, final double y2) {
        return new LineString(new PackedCoordinateSequence.Double(new double[] { x1, y1, x2, y2 }, 2), gf);
    }

    private static double[] bbox(final double x1, final double y1, final double x2, final double y2) {
        return new double[] { x1, y1, x1, y2, x2, y2, x2, y1, x1, y1 };
    }

    @Parameters
    public static Collection<Object[]> data() throws IOException {
        final Configuration conf = getConf();
        final List<RyaStatement> statements = getTestData();
        final List<GeoTemporalIndexer> indexers = new ArrayList<>();
        final List<Object[]> params = new ArrayList<>();
        for(final GeoTemporalIndexer indexer : indexers) {
            indexer.storeStatements(statements);
            params.add(new Object[]{conf, indexer});
        }
        return params;
    }

    private void test(final GeoTime geoTime, final GeoPolicy gPolicy, final TemporalPolicy tPolicy, final Set<Statement> expected) throws Exception {
        final CloseableIteration<Statement, QueryEvaluationException> actual = indexer.query(geoTime, gPolicy, tPolicy, EMPTY_CONSTRAINTS);
        final Set<Statement> set = new HashSet<>();
        while (actual.hasNext()) {
            set.add(actual.next());
        }
        assertEquals(expected, actual);
    }

    /*GEO POLICY IS EQUALS*/
    @Test
    public void testWithEquals_EqualsInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithEquals_BeforeInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(B, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithEquals_AfterInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(C, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.AFTER_INSTANT, expected);
    }

    @Test
    public void testWithEquals_BeforeInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(D, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_InInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(E, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.IN_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_AfterInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(F, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_StartInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(G, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.START_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_EndInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.END_INTERVAL, expected);
    }

    @Test
    public void testWithEquals_IntervalEquals() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithEquals_IntervalBefore() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithEquals_IntervalAfter() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.EQUALS, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS DISJOINT */
    @Test
    public void testWithDisjoint_EqualsInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithDisjoint_BeforeInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(B, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithDisjoint_AfterInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(C, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.AFTER_INSTANT, expected);
    }

    @Test
    public void testWithDisjoint_BeforeInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(D, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithDisjoint_InInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(E, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.IN_INTERVAL, expected);
    }

    @Test
    public void testWithDisjoint_AfterInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(F, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithDisjoint_StartInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(G, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.START_INTERVAL, expected);
    }

    @Test
    public void testWithDisjoint_EndInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.END_INTERVAL, expected);
    }

    @Test
    public void testWithDisjoint_IntervalEquals() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithDisjoint_IntervalBefore() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithDisjoint_IntervalAfter() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.DISJOINT, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS INTERSECTS */
    @Test
    public void testWithIntersects_EqualsInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithIntersects_BeforeInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(B, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithIntersects_AfterInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(C, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.AFTER_INSTANT, expected);
    }

    @Test
    public void testWithIntersects_BeforeInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(D, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_InInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(E, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.IN_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_AfterInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(F, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_StartInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(G, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.START_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_EndInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.END_INTERVAL, expected);
    }

    @Test
    public void testWithIntersects_IntervalEquals() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithIntersects_IntervalBefore() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithIntersects_IntervalAfter() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.INTERSECTS, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS TOUCHES */
    @Test
    public void testWithTouches_EqualsInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithTouches_BeforeInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(B, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithTouches_AfterInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(C, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.AFTER_INSTANT, expected);
    }

    @Test
    public void testWithTouches_BeforeInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(D, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithTouches_InInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(E, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.IN_INTERVAL, expected);
    }

    @Test
    public void testWithTouches_AfterInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(F, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithTouches_StartInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(G, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.START_INTERVAL, expected);
    }

    @Test
    public void testWithTouches_EndInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.END_INTERVAL, expected);
    }

    @Test
    public void testWithTouches_IntervalEquals() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithTouches_IntervalBefore() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithTouches_IntervalAfter() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.TOUCHES, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS CROSSES */
    @Test
    public void testWithCrosses_EqualsInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithCrosses_BeforeInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(B, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithCrosses_AfterInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(C, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.AFTER_INSTANT, expected);
    }

    @Test
    public void testWithCrosses_BeforeInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(D, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithCrosses_InInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(E, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.IN_INTERVAL, expected);
    }

    @Test
    public void testWithCrosses_AfterInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(F, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithCrosses_StartInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(G, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.START_INTERVAL, expected);
    }

    @Test
    public void testWithCrosses_EndInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.END_INTERVAL, expected);
    }

    @Test
    public void testWithCrosses_IntervalEquals() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithCrosses_IntervalBefore() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithCrosses_IntervalAfter() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CROSSES, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS WITHIN */
    @Test
    public void testWithWithin_EqualsInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithWithin_BeforeInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(B, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithWithin_AfterInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(C, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.AFTER_INSTANT, expected);
    }

    @Test
    public void testWithWithin_BeforeInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(D, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_InInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(E, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.IN_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_AfterInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(F, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_StartInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(G, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.START_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_EndInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.END_INTERVAL, expected);
    }

    @Test
    public void testWithWithin_IntervalEquals() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithWithin_IntervalBefore() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithWithin_IntervalAfter() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.WITHIN, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS CONTAINS */
    @Test
    public void testWithContains_EqualsInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithContains_BeforeInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(B, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithContains_AfterInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(C, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.AFTER_INSTANT, expected);
    }

    @Test
    public void testWithContains_BeforeInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(D, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithContains_InInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(E, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.IN_INTERVAL, expected);
    }

    @Test
    public void testWithContains_AfterInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(F, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithContains_StartInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(G, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.START_INTERVAL, expected);
    }

    @Test
    public void testWithContains_EndInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.END_INTERVAL, expected);
    }

    @Test
    public void testWithContains_IntervalEquals() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithContains_IntervalBefore() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithContains_IntervalAfter() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.CONTAINS, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    /* GEO POLICY IS OVERLAPS */
    @Test
    public void testWithOverlaps_EqualsInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.EQUALS_INSTANT, expected);
    }

    @Test
    public void testWithOverlaps_BeforeInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(B, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.BEFORE_INSTANT, expected);
    }

    @Test
    public void testWithOverlaps_AfterInstant() throws Exception {
        final GeoTime geoTime = new GeoTime(C, INSTANT);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.AFTER_INSTANT, expected);
    }

    @Test
    public void testWithOverlaps_BeforeInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(D, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.BEFORE_INTERVAL, expected);
    }

    @Test
    public void testWithOverlaps_InInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(E, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.IN_INTERVAL, expected);
    }

    @Test
    public void testWithOverlaps_AfterInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(F, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.AFTER_INTERVAL, expected);
    }

    @Test
    public void testWithOverlaps_StartInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(G, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.START_INTERVAL, expected);
    }

    @Test
    public void testWithOverlaps_EndInterval() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.END_INTERVAL, expected);
    }

    @Test
    public void testWithOverlaps_IntervalEquals() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.INTERVAL_EQUALS, expected);
    }

    @Test
    public void testWithOverlaps_IntervalBefore() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.INTERVAL_BEFORE, expected);
    }

    @Test
    public void testWithOverlaps_IntervalAfter() throws Exception {
        final GeoTime geoTime = new GeoTime(A, INTERVAL);
        final Set<Statement> expected = new HashSet<>();

        test(geoTime, GeoPolicy.OVERLAPS, TemporalPolicy.INTERVAL_AFTER, expected);
    }

    private static List<StatementPattern> getSPs(final String sparql) throws MalformedQueryException {
        final StatementPatternCollector spCollector = new StatementPatternCollector();
        new SPARQLParser().parseQuery(sparql, null).getTupleExpr().visit(spCollector);
        return spCollector.getStatementPatterns();
    }
}
