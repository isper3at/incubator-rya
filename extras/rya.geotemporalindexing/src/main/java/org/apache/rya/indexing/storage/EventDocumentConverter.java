package org.apache.rya.indexing.storage;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter;
import org.apache.rya.indexing.entity.storage.mongo.RyaTypeDocumentConverter;
import org.apache.rya.indexing.model.Event;
import org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy;
import org.apache.rya.indexing.mongodb.temporal.TemporalMongoDBStorageStrategy;
import org.bson.Document;
import org.joda.time.DateTime;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class EventDocumentConverter implements DocumentConverter<Event>{
    public static final String SUBJECT = "_id";
    public static final String GEO_KEY = "location";
    public static final String INTERVAL_START = "start";
    public static final String INTERVAL_END = "end";
    public static final String INSTANT = "instant";

    private final RyaTypeDocumentConverter ryaTypeConverter = new RyaTypeDocumentConverter();
    private final GeoMongoDBStorageStrategy geoAdapter = new GeoMongoDBStorageStrategy(0);
    private final TemporalMongoDBStorageStrategy temporalAdapter = new TemporalMongoDBStorageStrategy();

    @Override
    public Document toDocument(final Event event) {
        requireNonNull(event);

        final Document doc = new Document();
        doc.append(SUBJECT, event.getSubject().getData());
        doc.append(GEO_KEY, geoAdapter.getCorrespondingPoints(event.getGeometry()));
        if(event.isInstant()) {
            doc.append(INSTANT, event.getInstant().getAsDateTime());
        } else {
            doc.append(INTERVAL_START, event.getInterval().getHasBeginning().getAsDateTime());
            doc.append(INTERVAL_END, event.getInterval().getHasEnd().getAsDateTime());
        }

        return doc;
    }

    @Override
    public Event fromDocument(final Document document) throws DocumentConverterException {
        requireNonNull(document);

        final boolean isInstant;

        // Preconditions.
        if(!document.containsKey(SUBJECT)) {
            throw new DocumentConverterException("Could not convert document '" + document +
                    "' because its '" + SUBJECT + "' field is missing.");
        }

        if(!document.containsKey(GEO_KEY)) {
            throw new DocumentConverterException("Could not convert document '" + document +
                    "' because its '" + GEO_KEY + "' field is missing.");
        }

        if(document.containsKey(INSTANT)) {
            isInstant = true;
        } else {
            isInstant = false;
            if(!document.containsKey(INTERVAL_START) && !document.containsKey(INTERVAL_END)) {
                throw new DocumentConverterException("Could not convert document '" + document +
                        "' because its '" + INTERVAL_START + "' and '" + INTERVAL_END + "' fields are missing.");
            }
        }

        final String subject = document.getString(SUBJECT);
        final List<double[]> points = (List<double[]>) document.get(GEO_KEY);
        final Coordinate[] coords = new Coordinate[points.size()];
        for(int ii = 0; ii < points.size(); ii++) {
            final double[] point = points.get(ii);
            coords[ii] = new Coordinate(point[0], point[1]);
        }
        final GeometryFactory geoFact = new GeometryFactory();
        final Geometry geo = geoFact.createPolygon(coords);

        final Event.Builder builder = new Event.Builder()
                .setSubject(new RyaURI(subject))
                .setGeometry(geo);
        if(isInstant) {
            final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.parse(document.getString(INSTANT), TemporalInstantRfc3339.FORMATTER));
            builder.setTemporalInstant(instant);
        } else {
            final TemporalInstant begining = new TemporalInstantRfc3339(DateTime.parse(document.getString(INTERVAL_START), TemporalInstantRfc3339.FORMATTER));
            final TemporalInstant end = new TemporalInstantRfc3339(DateTime.parse(document.getString(INTERVAL_END), TemporalInstantRfc3339.FORMATTER));
            final TemporalInterval interval = new TemporalInterval(begining, end);
            builder.setTemporalInterval(interval);
        }
        return builder.build();
    }

}
