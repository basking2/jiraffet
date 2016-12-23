package com.github.basking2.otternet;

import java.io.IOException;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.wadl.WadlFeature;

public class OtterNet {
    public static final void main(final String[] argv) throws InterruptedException, IOException {

        new OtterNet().start();

        Thread.currentThread().join();
    }
    
    public void start() throws IOException {
        HttpServer httpServer = new HttpServer();
        NetworkListener networkListener = new NetworkListener("otter", "0.0.0.0", 8765);
        httpServer.addListener(networkListener);
        
        HttpHandler dynamicHandler = ContainerFactory.createContainer(HttpHandler.class, resourceConfig());

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
}