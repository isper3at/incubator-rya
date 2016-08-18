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
package org.apache.rya.export.api.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.rya.export.DBType;
import org.apache.rya.export.JAXBMergeConfiguration;
import org.apache.rya.export.MergePolicy;
import org.apache.rya.export.api.conf.MergeConfiguration.Builder;

import com.google.common.annotations.VisibleForTesting;

/**
 * Helper class for processing command line arguments for the Merge Tool.
 */
public class MergeConfigurationCLI {
    public static final Option CONFIG_OPTION = new Option("c", true, "Defines the configuration file for the Merge Tool to use.");

    /**
     * @return The valid {@link Options}
     */
    @VisibleForTesting
    public static Options getOptions() {
        final Options cliOptions = new Options()
        .addOption(CONFIG_OPTION);
        return cliOptions;
    }

    @VisibleForTesting
    public static <T extends JAXBMergeConfiguration> T createConfigurationFromFile(final File configFile, final Class<?> mergeConfigurationClass) throws MergeConfigurationException {
        try (final FileInputStream configFileInputStream = new FileInputStream(configFile)) {
            return createConfigurationFromStream(configFileInputStream, mergeConfigurationClass);
        } catch (final IOException e) {
             throw new MergeConfigurationException("Failed to create a config based on the provided configuration.", e);
        }
    }

    @VisibleForTesting
    public static <T extends JAXBMergeConfiguration> T createConfigurationFromStream(final InputStream configInputStream, final Class<?> mergeConfigurationClass) throws MergeConfigurationException {
        XMLStreamReader xsr = null;
        try {
            final XMLInputFactory xif = XMLInputFactory.newFactory();
            final StreamSource xml = new StreamSource(configInputStream);
            xsr = xif.createXMLStreamReader(xml);

            final JAXBContext context = JAXBContext.newInstance(DBType.class, MergePolicy.class, mergeConfigurationClass);
            final Unmarshaller unmarshaller = context.createUnmarshaller();
            final JAXBElement<?> jaxbElement = unmarshaller.unmarshal(xsr, mergeConfigurationClass);

            return (T) jaxbElement.getValue();
        } catch (final JAXBException | IllegalArgumentException | XMLStreamException e) {
            throw new MergeConfigurationException("Failed to create a config based on the provided configuration.", e);
        } finally {
            try {
                if (xsr != null) {
                    xsr.close();
                }
            } catch (final XMLStreamException e) {
                throw new MergeConfigurationException("Failed to close configuration file.", e);
            }
        }
    }

    public static MergeConfiguration createConfiguration(final String[] args, final ConfigurationAdapter<? extends Builder, ? extends JAXBMergeConfiguration> configurationAdapter, final Class<? extends JAXBMergeConfiguration> configurationClass) throws MergeConfigurationException {
        final Options cliOptions = getOptions();
        final CommandLineParser parser = new BasicParser();
        try {
            final CommandLine cmd = parser.parse(cliOptions, args);
            //If the config option is present, ignore all other options.
            if(cmd.hasOption(CONFIG_OPTION.getOpt())) {
                final File xml = new File(cmd.getOptionValue(CONFIG_OPTION.getOpt()));
                return configurationAdapter.createConfig(createConfigurationFromFile(xml, configurationClass));
            } else {
                throw new MergeConfigurationException("No configuration was provided.");
            }
        } catch (final ParseException pe) {
            throw new MergeConfigurationException("Improperly formatted options.", pe);
        }
    }
}
