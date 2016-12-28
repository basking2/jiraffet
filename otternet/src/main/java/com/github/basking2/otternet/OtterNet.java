package com.github.basking2.otternet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.github.basking2.otternet.http.ControlService;
import com.github.basking2.otternet.util.App;
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

    private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
    private OtterIO io = new OtterIO(Ip.whatsMyIp(), new ArrayList<>());
    private OtterLog log = new OtterLog(this, io);
    final Jiraffet jiraffet = new Jiraffet(log, io);
    final App otterNetApp;
    final Configuration config;

    public static final void main(final String[] argv) throws InterruptedException, IOException, ConfigurationException {
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
    
    public OtterNet() throws ConfigurationException {
        this.httpServer = new HttpServer();
        this.otterNetApp = new App("otternet");
        this.config = otterNetApp.buildConfiguration();
    }
    
    public void start() throws IOException {
        final NetworkListener networkListener =
            new NetworkListener("otter", config.getString("otternet.addr"), config.getInt("otternet.port"));

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
        rc.register(new ControlService(io, log));
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