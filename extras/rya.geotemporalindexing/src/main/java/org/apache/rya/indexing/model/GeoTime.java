package org.apache.rya.indexing.model;

import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInterval;

import com.vividsolutions.jts.geom.Geometry;

public class GeoTime {
    private final Geometry geometry;
    private TemporalInstant instant;
    private TemporalInterval interval;

    private final boolean isInstant;

    public GeoTime(final Geometry geo, final TemporalInstant instant) {
        isInstant = true;
        geometry = geo;
        this.instant = instant;
    }

    public GeoTime(final Geometry geo, final TemporalInterval interval) {
        isInstant = false;
        geometry = geo;
        this.interval = interval;
    }

    public boolean isInstant() {
        return isInstant;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    public TemporalInstant getInstant() {
        return instant;
    }

    public TemporalInterval getInterval() {
        return interval;
    }
}
