package com.github.basking2.jiraffet.util;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

/**
 * Some nice defaults for configuraing an application.
 */
public class AppConfiguration  extends CompositeConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(AppConfiguration.class);

    final protected String name;

    public AppConfiguration(final String name){
        this.name = name;
        addDefaults();
    }
    public AppConfiguration(final Class<?> clazz){
        this(clazz.getSimpleName().toLowerCase());
    }

    public String getName() {
        return name;
    }

    public void loadOptionalFile(final String prefix, final String file) {
        // Load an optional properties file.
        try {
            final File properties = new File(prefix, file);
            if (properties.canRead()) {
                final PropertiesConfiguration fileProperties = new PropertiesConfiguration();
                fileProperties.append(new SystemConfiguration());

                try (final InputStream is = new FileInputStream(properties)) {
                    fileProperties.load(is);
                }
                catch (final IOException e ){
                    LOG.error("Loading configuration file: "+ properties.getAbsolutePath(), e);
                }

                addConfiguration(fileProperties);
            }
        }
        catch (final ConfigurationException e) {
            // Nop.
        }

    }

    public void loadOptionalResource(final String resource, final Class<?> clazz) {
        // Load classpath properties.
        try {
            final PropertiesConfiguration classpathProperties = new PropertiesConfiguration();
            classpathProperties.append(new SystemConfiguration());
            classpathProperties.load(clazz.getResourceAsStream(resource));
            addConfiguration(classpathProperties);
        } catch (final ConfigurationException | NullPointerException e) {
            LOG.error("Failed to load reasource: "+resource, e);
        }
    }

    public void addDefaults() {
        // Add system properties.
        addConfiguration(new SystemConfiguration());

        // Load an optional properties file.
        loadOptionalFile(".", name + ".properties");

        // Load an optional properties file.
        loadOptionalFile("/etc", name + ".properties");

        // Load an optional properties file.
        loadOptionalFile("/etc/"+name, name + ".properties");

        // Load an optional properties file.
        loadOptionalFile("/opt/"+name+"/conf", name+".properties");

        // Load classpath properties.
        loadOptionalResource(String.format("/%s.properties", name), getClass());
    }

}
