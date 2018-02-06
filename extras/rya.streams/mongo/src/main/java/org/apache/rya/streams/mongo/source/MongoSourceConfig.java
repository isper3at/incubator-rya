package org.apache.rya.streams.mongo.source;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class MongoSourceConfig extends AbstractConfig {
    public static final String HOST = "host";
    private static final String HOST_DOC = "Host url of mongodb";
    public static final String PORT = "port";
    private static final String PORT_DOC = "Port of mongodb";
    public static final String URI = "uri";
    private static final String URI_DOC = "uri of mongodb";
    public static final String BATCH_SIZE = "batch.size";
    private static final String BATCH_SIZE_DOC = "Count of documents in each polling";
    public static final String TOPIC_PREFIX = "rya.instance";
    private static final String TOPIC_PREFIX_DOC = "Rya Instance name.  Used as a prefix of each topic, final topic will be prefix_db_collection";
    public static final String CONVERTER_CLASS = "converter.class";

    public static ConfigDef config = new ConfigDef()
            .define(URI, Type.STRING, Importance.HIGH, URI_DOC)
            .define(HOST, Type.STRING, Importance.HIGH, HOST_DOC)
            .define(PORT, Type.INT, Importance.HIGH, PORT_DOC)
            .define(BATCH_SIZE, Type.INT, Importance.HIGH, BATCH_SIZE_DOC)
            .define(TOPIC_PREFIX, Type.STRING, Importance.LOW, TOPIC_PREFIX_DOC);

    public MongoSourceConfig(final Map<String, String> props) {
        super(config, props);
    }
}