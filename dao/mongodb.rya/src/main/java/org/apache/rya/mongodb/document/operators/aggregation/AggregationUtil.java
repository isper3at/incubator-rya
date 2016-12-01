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
package org.apache.rya.mongodb.document.operators.aggregation;

import static org.apache.rya.mongodb.document.operators.aggregation.PipelineOperators.redact;
import static org.apache.rya.mongodb.document.operators.aggregation.SetOperators.setIntersection;
import static org.apache.rya.mongodb.document.operators.aggregation.SetOperators.setIsSubsetNullSafe;
import static org.apache.rya.mongodb.document.operators.query.ArrayOperators.size;
import static org.apache.rya.mongodb.document.operators.query.ComparisonOperators.gt;
import static org.apache.rya.mongodb.document.operators.query.LogicalOperators.or;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.mongodb.MongoDbRdfConstants;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.operators.aggregation.PipelineOperators.RedactAggregationResult;
import org.apache.rya.mongodb.document.util.AuthorizationsUtil;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Utility methods for MongoDB aggregation.
 */
public final class AggregationUtil {
    /**
     * Private constructor to prevent instantiation.
     */
    private AggregationUtil() {
    }

    /**
     * Creates a MongoDB $redact aggregation pipeline that only include
     * documents whose document visibility match the provided authorizations.
     * All other documents are excluded.
     * @param authorizations the {@link Authorization}s to include in the
     * $redact. Only documents that match the authorizations will be returned.
     * @return the {@link List} of {@link DBObject}s that represents the $redact
     * aggregation pipeline.
     */
    public static List<DBObject> createRedactPipeline(final Authorizations authorizations) {
        if (MongoDbRdfConstants.ALL_AUTHORIZATIONS.equals(authorizations)) {
            return Lists.newArrayList();
        }

        final List<String> authAndList = AuthorizationsUtil.getAuthorizationsStrings(authorizations);

        // Generate all combinations of the authorization strings without repetition.
        final List<List<String>> authOrList = createCombinations(AuthorizationsUtil.getAuthorizationsStrings(authorizations));

        final String documentVisibilityField = "$" + SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY;

        final BasicDBObject setIsSubset =
            setIsSubsetNullSafe(
                documentVisibilityField,
                authAndList.toArray()
            );

        final BasicDBObject setIntersectionExists =
            gt(
                size(
                    setIntersection(
                        documentVisibilityField,
                        authOrList.toArray()
                    )
                ),
                0
            );

        final BasicDBObject orExpression = or(setIsSubset, setIntersectionExists);

        final List<DBObject> pipeline = new ArrayList<>();
        pipeline.add(
            redact(
               orExpression,
               RedactAggregationResult.DESCEND,
               RedactAggregationResult.PRUNE
            )
        );

        return pipeline;
    }

    /**
     * Creates all combinations of the values that are of the size of value
     * array or smaller without repetition.
     * @param values the {@link List} of values to create combinations from.
     * @return the {@link List} of combinations.
     */
    private static <T> List<List<T>> createCombinations(final List<T> values) {
        final List<List<T>> allCombinations = new ArrayList<>();
        for (int i = 1; i <= values.size(); i++) {
            allCombinations.addAll(createCombinations(values, i));
        }
        return allCombinations;
    }

    /**
     * Creates all combinations of the values that are of the specified size
     * without repetition.
     * @param values the {@link List} of values to create combinations from.
     * @param size the size of the combinations.
     * @return the {@link List} of combinations.
     */
    private static <T> List<List<T>> createCombinations(final List<T> values, final int size) {
        if (0 == size) {
            return Collections.singletonList(Collections.<T> emptyList());
        }

        if (values.isEmpty()) {
            return Collections.emptyList();
        }

        final List<List<T>> combination = new LinkedList<List<T>>();

        final T actual = values.iterator().next();

        final List<T> subSet = new LinkedList<T>(values);
        subSet.remove(actual);

        final List<List<T>> subSetCombination = createCombinations(subSet, size - 1);

        for (final List<T> set : subSetCombination) {
            final List<T> newSet = new LinkedList<T>(set);
            newSet.add(0, actual);
            combination.add(newSet);
        }

        combination.addAll(createCombinations(subSet, size));

        return combination;
    }
}