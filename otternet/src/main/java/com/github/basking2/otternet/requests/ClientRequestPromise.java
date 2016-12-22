package com.github.basking2.otternet.requests;

import com.github.basking2.jiraffet.messages.ClientRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 */
public class ClientRequestPromise implements ClientRequest{
    private final byte[] data;
    private final CompletableFuture<ClientRequestResult> future;

    public ClientRequestPromise(final byte[] data) {
        this.data = data;
        this.future = new CompletableFuture<>();
    }

    public Future<ClientRequestResult> getFuture() {
        return future;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public void complete(boolean success, String leader, String msg) {
        future.complete(new ClientRequestResult(success, leader, msg));
    }

}
