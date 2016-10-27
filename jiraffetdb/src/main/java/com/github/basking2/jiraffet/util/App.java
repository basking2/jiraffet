package com.github.basking2.jiraffet.util;

import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A class to capture the notion of an application.
 */
public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    final protected String name;

    public App(final String name){
        this.name = name;
    }
    public App(final Class<?> clazz){
        this(clazz.getSimpleName().toLowerCase());
    }

    public String getName() {
        return name;
    }

    public Configuration buildConfiguration() throws ConfigurationException {

        final Configurations configurations = new Configurations();

        final List<Configuration> configs = new ArrayList<>();

        // Check system first to allow overridding on the command line.
        configs.add(new SystemConfiguration());

        // Check ./name.properties first.
        configs.add(configurations.properties(new File("./"+name+".properties")));

        // System configs. Order isn't cruicial.
        configs.add(configurations.properties(new File("/etc/"+name+".properties")));
        configs.add(configurations.properties(new File("/etc/"+name+"/"+name+".properties")));
        configs.add(configurations.properties(new File("/opt/"+name+"/conf/"+name+".properties")));

        // Embedded is last.
        configs.add(configurations.properties(getClass().getResource("/"+name+".properties")));

        return new CompositeConfiguration(configs);
    }

}
