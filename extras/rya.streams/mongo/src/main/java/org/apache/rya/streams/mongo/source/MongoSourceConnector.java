package org.apache.rya.streams.mongo.source;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class MongoSourceConnector extends SourceConnector {

    @Override
    public ConfigDef config() {
        return MongoSourceConfig.config;
    }

    @Override
    public void start(final Map<String, String> arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String version() {
        // TODO Auto-generated method stub
        return null;
    }

}
