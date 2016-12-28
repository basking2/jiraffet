package com.github.basking2.otternet.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ip {
    public static String whatsMyIp() {
        try {
            final URL url = new URL("https://api.ipify.org/?format=json");
            final ObjectMapper objectMapper = new ObjectMapper();

            @SuppressWarnings("unchecked")
            final HashMap<String, String> m = (HashMap<String, String>)objectMapper.readValue(
                    url.openConnection().getInputStream(),
                    HashMap.class);

            return m.get("ip");
        }
        catch (final MalformedURLException e) {
            throw new RuntimeException(e);
        } catch (JsonParseException e) {
            throw new RuntimeException(e);
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
