package com.github.basking2.otternet;

import java.io.IOException;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.wadl.WadlFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtterNet implements AutoCloseable {
    final HttpServer httpServer;
    private static final Logger LOG = LoggerFactory.getLogger(OtterNet.class);
    
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
    }
    
    public ResourceConfig resourceConfig() {
        ResourceConfig rc = new ResourceConfig();
        rc.packages("com.github.basking2.otternet.http");
        rc.register(WadlFeature.class);
        rc.register(JacksonFeature.class);
        //rc.register(HTTPResponseFilter.class);
        return rc;
    }

    @Override
    public void close() throws Exception {
        httpServer.shutdown();
    }
}