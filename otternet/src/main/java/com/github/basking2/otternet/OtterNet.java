package com.github.basking2.otternet;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.github.basking2.jiraffet.Jiraffet;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.otternet.jiraffet.OtterLog;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.wadl.WadlFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.otternet.http.JiraffetJson;
import com.github.basking2.otternet.jiraffet.OtterIO;

public class OtterNet implements AutoCloseable {
    final HttpServer httpServer;
    private static final Logger LOG = LoggerFactory.getLogger(OtterNet.class);

    private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
    private OtterIO io = new OtterIO(null, null);
    private OtterLog log = new OtterLog(this, io);
    final Jiraffet jiraffet = new Jiraffet(log, io);

    public static final void main(final String[] argv) throws InterruptedException, IOException {
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
    
    public OtterNet() {
        this.httpServer = new HttpServer();
    }
    
    public void start() throws IOException {
        final NetworkListener networkListener = new NetworkListener("otter", "0.0.0.0", 8765);
        
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

        jiraffetThread.start();
        jiraffetThread.setDaemon(true);

    }

    public void join(final String cluster) throws MalformedURLException {
        final URL url = new URL(cluster);
    }

    public void leave(final String cluster) throws MalformedURLException {
        final URL url = new URL(cluster);
    }

    public ResourceConfig resourceConfig() {
        final ResourceConfig rc = new ResourceConfig();
        rc.register(new JiraffetJson(io, log));
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