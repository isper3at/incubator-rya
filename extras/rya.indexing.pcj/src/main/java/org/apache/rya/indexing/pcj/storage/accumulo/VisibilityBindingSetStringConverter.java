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
package org.apache.rya.indexing.pcj.storage.accumulo;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.query.BindingSet;

import com.google.common.base.Strings;

/**
 * Converts {@link BindingSet}s to Strings and back again. The Strings do not
 * include the binding names and are ordered with a {@link VariableOrder}.
 */
@ParametersAreNonnullByDefault
public class VisibilityBindingSetStringConverter extends BindingSetStringConverter {
    public static final char VISIBILITY_DELIM = 1;

    @Override
    public String convert(final BindingSet bindingSet, final VariableOrder varOrder) {
        String visibility = "";
        if(bindingSet instanceof VisibilityBindingSet) {
            final VisibilityBindingSet visiSet = (VisibilityBindingSet) bindingSet;
            if(!Strings.isNullOrEmpty(visiSet.getVisibility())) {
                visibility = VISIBILITY_DELIM + visiSet.getVisibility();
            }
        }
        return super.convert(bindingSet, varOrder) + visibility;
    }

    @Override
    public BindingSet convert(final String bindingSetString, final VariableOrder varOrder) {
        final String[] visiStrings = bindingSetString.split("" + VISIBILITY_DELIM);
        BindingSet bindingSet = super.convert(visiStrings[0], varOrder);

        if(visiStrings.length > 1) {
            bindingSet = new VisibilityBindingSet(bindingSet, visiStrings[1]);
        } else {
            bindingSet = new VisibilityBindingSet(bindingSet);
        }
        return bindingSet;
    }
}