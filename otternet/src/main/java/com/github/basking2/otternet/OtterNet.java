package com.github.basking2.otternet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.github.basking2.otternet.http.ControlService;
import com.github.basking2.otternet.util.App;
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.glassfish.grizzly.http.server.*;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.wadl.WadlFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.Jiraffet;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.otternet.http.JiraffetJsonService;
import com.github.basking2.otternet.jiraffet.OtterIO;
import com.github.basking2.otternet.jiraffet.OtterLog;
import com.github.basking2.otternet.util.Ip;

/**
 * Startup Main line that does surprisingly little of the work and magic.
 * 
 * @see OtterIO
 * @see OtterLog
 */
public class OtterNet implements AutoCloseable {
    final HttpServer httpServer;
    private static final Logger LOG = LoggerFactory.getLogger(OtterNet.class);
    static private App otterNetApp;

    final private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
    final private OtterIO io;
    final private OtterLog log;
    final private Jiraffet jiraffet;
    final private Configuration config;

    public static final void main(final String[] argv) throws InterruptedException, IOException, ConfigurationException, ParseException {

        // Parse CLI arguments.
        mainParseArgs(argv);

        otterNetApp = new App("otternet");

        final OtterNet otterNet = new OtterNet();
        otterNet.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try {
                otterNet.close();
            }
            catch (final Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }));

        Thread.currentThread().join();
    }

    /**
     * Parse the command line arguments into System properties to be picked up by the application.
     *
     * @param argv The user's command line options.
     * @throws ParseException On a parser exception.
     */
    public static void mainParseArgs(final String[] argv) throws ParseException {
        //================================================================================
        // Option parsing
        //================================================================================
        final CommandLineParser cliParser = new DefaultParser();
        final Options cliOptions = new Options().
                addOption("p", "port", true, "Port to listen on.").
                addOption("i", "ip", true, "IP to bind to. If set to \"auto\" an external site is used.").
                addOption("I", "id", true, "http://address:port id used to locate this host and join clusters.").
                addOption("H", "home", true, "Home directory.").
                addOption("h", "help", false, "Help.");
        final CommandLine commandLine = cliParser.parse(cliOptions, argv);

        if (commandLine.hasOption('h')) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("otternet", "", cliOptions, "\nExiting.\n", true);
            System.exit(1);
        }

        // Move all of our command line parsed options into the system properties where we'll pick them up.
        if (commandLine.hasOption("p")) {
            System.setProperty("otternet.port", commandLine.getOptionValue("p"));
        }
        if (commandLine.hasOption("i")) {
            System.setProperty("otternet.addr", commandLine.getOptionValue("i"));
        }
        if (commandLine.hasOption("I")) {
            System.setProperty("otternet.id", commandLine.getOptionValue("I"));
        }
        if (commandLine.hasOption("H")) {
            System.setProperty("otternet.home", commandLine.getOptionValue("I"));
        }
        //================================================================================
    }

    public OtterNet() throws ConfigurationException {
        config = otterNetApp.buildConfiguration();

        String ip = config.getString("otternet.addr", "0.0.0.0");
        if ("auto".equalsIgnoreCase(ip)) {
            ip = Ip.whatsMyIp();
        }

        int port = config.getInt("otternet.port", 8080);

        String id = "http://"+ip +":"+port;
        if (config.getString("otter.id", null) != null) {
            id = config.getString("otter.id");
        }

        LOG.info("Starting OtterNet bound to {}:{} with id {}.", ip, port, id);

        io = new OtterIO(id, new ArrayList<>());
        log = new OtterLog(this, io);
        jiraffet = new Jiraffet(log, io);
        httpServer = new HttpServer();

        final NetworkListener networkListener = new NetworkListener("otter", ip, port);

        /*
        networkListener.setDefaultErrorPageGenerator(new ErrorPageGenerator() {
            @Override
            public String generate(final Request request, final int status, final String reasonPhrase, final String description, final Throwable exception) {
                return new StringBuilder().
                        append("<html><body>").
                        append("<h1>").append(reasonPhrase).append("</h1>").
                        append("<div><b>").append(description).append("</b></div>").
                        append("<code>").append(exception).append("</code>").
                        append("</body></html>").
                        toString();
            }
        });
        */

        final HttpHandler dynamicHandler = ContainerFactory.createContainer(HttpHandler.class, resourceConfig());

        httpServer.addListener(networkListener);

        httpServer.getServerConfiguration().addHttpHandler(dynamicHandler, "/");

    }
    
    public void start() throws IOException {

        httpServer.start();

        final Thread jiraffetThread = new Thread(() -> {
                try {
                    jiraffet.run();
                } catch (JiraffetIOException e) {
                    e.printStackTrace();
                }
                return;
            });

        jiraffetThread.setDaemon(true);
        jiraffetThread.start();

    }

    public ResourceConfig resourceConfig() {
        final ResourceConfig rc = new ResourceConfig();
        rc.register(new JiraffetJsonService(io, log));
        rc.register(new ControlService(jiraffet, io, log));
        rc.register(WadlFeature.class);
        rc.register(JacksonFeature.class);
        rc.packages("com.github.basking2.otternet.http.scanned");
        //rc.register(HTTPResponseFilter.class);
        return rc;
    }

    @Override
    public void close() throws Exception {
        try {
            httpServer.shutdown();
        }
        catch (final Throwable t) {
            // Nope
        }

        try {
            jiraffet.shutdown();
        }
        catch (final Throwable t) {
            // Nop.
        }
    }
}