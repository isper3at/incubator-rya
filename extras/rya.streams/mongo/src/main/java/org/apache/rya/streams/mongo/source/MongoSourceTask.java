package org.apache.rya.streams.mongo.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka connect task
 */
public class MongoSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(MongoSourceTask.class);

    @Override
    public String version() {
        return null;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final List<SourceRecord> records = new ArrayList<>();
        while (!reader.isEmpty()) {
            final Document message = reader.pool();
            final Struct messageStruct = getStruct(message);
            final String topic = getTopic(message);
            final String db = getDB(message);
            final String timestamp = getTimestamp(message);
            records.add(new SourceRecord(Collections.singletonMap("mongodb", db), Collections.singletonMap(db, timestamp), topic, messageStruct.schema(), messageStruct));
            log.trace(message.toString());
        }


        return records;
    }

    @Override
    public void start(final Map<String, String> arg0) {
    }

    @Override
    public void stop() {
    }
}
