package com.github.basking2.otternet;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.wadl.WadlFeature;

public class OtterNet {
    public static final void main(final String[] argv) {

        new OtterNet().foo();
    }
    
    public void foo() {
        ResourceConfig rc = new ResourceConfig();
        rc.packages("com.github.basking2.otternet.http");
        rc.register(WadlFeature.class);
        rc.register(JacksonFeature.class);
        //rc.register(HTTPResponseFilter.class);
    }
}