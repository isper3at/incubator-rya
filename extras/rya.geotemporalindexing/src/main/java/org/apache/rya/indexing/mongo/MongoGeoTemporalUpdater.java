package org.apache.rya.indexing.mongo;

import static java.util.Objects.requireNonNull;

import java.util.Optional;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.model.Event;
import org.apache.rya.indexing.mongodb.IndexingException;
import org.apache.rya.indexing.mongodb.update.DocumentUpdater;
import org.apache.rya.indexing.storage.EventStorage;

public class MongoGeoTemporalUpdater implements DocumentUpdater<RyaURI, Event>{

    private final EventStorage events;

    public MongoGeoTemporalUpdater(final EventStorage events) {
        this.events = requireNonNull(events);
    }

    @Override
    public Optional<Event> getOld(final RyaURI key) throws IndexingException {
        return null;
    }

    @Override
    public void create(final Event newObj) throws IndexingException {
    }

    @Override
    public void update(final Event old, final Event updated) throws IndexingException {
    }
}
