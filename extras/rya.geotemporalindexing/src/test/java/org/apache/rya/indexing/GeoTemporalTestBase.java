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

import static org.junit.Assert.assertEquals;

import org.junit.ComparisonFailure;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

public class GeoTemporalTestBase {
    private static final GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);

    /**
     * Make an uniform instant with given seconds.
     */
    protected static TemporalInstant makeInstant(final int secondsMakeMeUnique) {
        return new TemporalInstantRfc3339(2015, 12, 30, 12, 00, secondsMakeMeUnique);
    }

    protected static Polygon poly(final double[] arr) {
        final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(arr, 2));
        final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
        return p1;
    }

    protected static Point point(final double x, final double y) {
        return gf.createPoint(new Coordinate(x, y));
    }

    protected static LineString line(final double x1, final double y1, final double x2, final double y2) {
        return new LineString(new PackedCoordinateSequence.Double(new double[] { x1, y1, x2, y2 }, 2), gf);
    }

    protected static double[] bbox(final double x1, final double y1, final double x2, final double y2) {
        return new double[] { x1, y1, x1, y2, x2, y2, x2, y1, x1, y1 };
    }

    protected void assertEqualMongo(final Object expected, final Object actual) throws ComparisonFailure {
        try {
            assertEquals(expected, actual);
        } catch(final Throwable e) {
            throw new ComparisonFailure(e.getMessage(), expected.toString(), actual.toString());
        }
    }
}
