package org.apache.rya.indexing;

import org.apache.rya.api.persist.index.RyaSecondaryIndexer;
import org.apache.rya.indexing.model.GeoTime;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;

import info.aduna.iteration.CloseableIteration;

/**
 * A repository to store, index, and retrieve {@link Statement}s based on geotemporal features.
 */
public interface GeoTemporalIndexer extends RyaSecondaryIndexer {
    /**
     * Returns statements that contain occured based in a geometry and at a time.
     * <br>
     * The geometry properties are based on the {@link GeoPolicy}.
     * <br>
     * The temporal properties are based on the {@link TemporalPolicy}.
     *
     * @param geoTime - The {@link GeoTime}
     * @param geoPolicy
     * @param temporalPolicy
     * @param contraints
     *
     * @return An iteration of {@link Statement}s matching the query.
     * @see TemporalIndexer
     * @see GeoIndexer
     */
    public abstract CloseableIteration<Statement, QueryEvaluationException> query(final GeoTime geoTime, final GeoPolicy geoPolicy, final TemporalPolicy temporalPolicy, final StatementConstraints contraints);


    public enum GeoPolicy {
        EQUALS,
        DISJOINT,
        INTERSECTS,
        TOUCHES,
        CROSSES,
        WITHIN,
        CONTAINS,
        OVERLAPS;
    }

    public enum TemporalPolicy {
        EQUALS_INSTANT(true),
        BEFORE_INSTANT(true),
        AFTER_INSTANT(true),
        BEFORE_INTERVAL(false),
        IN_INTERVAL(false),
        AFTER_INTERVAL(false),
        START_INTERVAL(false),
        END_INTERVAL(false),
        INTERVAL_EQUALS(false),
        INTERVAL_BEFORE(false),
        INTERVAL_AFTER(false);

        private final boolean isInstant;

        TemporalPolicy(final boolean isInstant) {
            this.isInstant = isInstant;
        }

        public boolean isInstant(){
            return isInstant;
        }
    }
}
