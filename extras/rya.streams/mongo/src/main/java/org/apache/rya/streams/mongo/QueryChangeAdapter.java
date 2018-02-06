package org.apache.rya.streams.mongo;

import org.apache.rya.streams.api.queries.ChangeLogEntry;
import org.apache.rya.streams.api.queries.QueryChange;
import org.bson.Document;

/**
 * Adapter for changing {@link QueryChange} {@link ChangeLogEntry}s into a {@link Document} or
 * a {@link Document} into {@link QueryChange} {@link ChangeLogEntry}s.
 *
 * The {@link Document} will be stored or retrieved in a mongoDB collection, defined by
 * {@link MongoKafkaOpLogCollection}.
 */
public class QueryChangeAdapter {
    public static final String STATEMENT_KEY = "statement";

    public Document toDocument() {
        return new Document();
    }
}
