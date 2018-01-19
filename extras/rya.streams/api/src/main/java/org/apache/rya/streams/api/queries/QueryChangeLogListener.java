package org.apache.rya.streams.api.queries;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Listener to be notified when {@link QueryChange}s occurr on a {@link QueryChangeLog}
 */
@DefaultAnnotation(NonNull.class)
public interface QueryChangeLogListener {
    /**
     * Notifies the listener that a query change event has occurred in the change log.
     * @param queryChangeEvent - The event that occurred. (not null)
     */
    public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent);
}
