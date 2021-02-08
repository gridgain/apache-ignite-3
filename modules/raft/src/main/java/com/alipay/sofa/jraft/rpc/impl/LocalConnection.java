package com.alipay.sofa.jraft.rpc.impl;

import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.Message;
import com.alipay.sofa.jraft.util.Endpoint;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;

public class LocalConnection implements Connection {
    private static boolean RECORD_ALL_MESSAGES = false;

    private Map<String, Object> attrs = new ConcurrentHashMap<>();

    public final LocalRpcClient client;
    public final LocalRpcServer srv;

    private volatile Predicate<Message> recordPred;
    private volatile Predicate<Message> blockPred;

    private LinkedBlockingQueue<Object[]> blockedMsgs = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Message> recordedMsgs = new LinkedBlockingQueue<>();

    public LocalConnection(LocalRpcClient client, LocalRpcServer srv) {
        this.client = client;
        this.srv = srv;
    }

    public void recordMessages(Predicate<Message> pred) {
        this.recordPred = pred;
    }

    public void blockMessages(Predicate<Message> pred) {
        this.blockPred = pred;
    }

    private void send(Message request, Future fut) {
        Object[] tuple = {client, request, fut};
        assert srv.incoming.offer(tuple); // Should never fail because server uses unbounded queue.
    }

    public void onBeforeRequestSend(Message request, Future fut) {
        if (RECORD_ALL_MESSAGES || recordPred != null && recordPred.test(request))
            recordedMsgs.add(request);

        if (blockPred != null && blockPred.test(request)) {
            blockedMsgs.add(new Object[]{request, fut});

            return;
        }

        send(request, fut);
    }

    public void sendBlocked() {
        blockedMsgs.drainTo(srv.incoming);
    }

    public void onAfterResponseSend(Message msg, Throwable err) {
        assert err == null : err;

        if (RECORD_ALL_MESSAGES || recordPred != null && recordPred.test(msg))
            recordedMsgs.add(msg);
    }

    @Override public Object getAttribute(String key) {
        return attrs.get(key);
    }

    @Override public void setAttribute(String key, Object value) {
        attrs.put(key, value);
    }

    @Override public Object setAttributeIfAbsent(String key, Object value) {
        return attrs.putIfAbsent(key, value);
    }

    @Override public void close() {
        LocalRpcServer.closeConnection(client, srv.local);
    }

    public Queue<Message> recordedMessages() {
        return recordedMsgs;
    }

    @Override public String toString() {
        return client.toString() + " -> " + srv.local.toString();
    }
}
