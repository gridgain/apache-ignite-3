/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rpc.impl;

import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.Message;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local RPC server impl.
 *
 * @author ascherbakov.
 */
public class LocalRpcServer implements RpcServer {
    private static final Logger LOG                    = LoggerFactory.getLogger(LocalRpcServer.class);

    /** Running servers. */
    public static ConcurrentMap<Endpoint, LocalRpcServer> servers = new ConcurrentHashMap<>();

    Endpoint local;

    /** Remote connections to this server. */
    public ConcurrentMap<LocalRpcClient, LocalConnection> conns = new ConcurrentHashMap<>();

    private Map<String, RpcProcessor> processors = new ConcurrentHashMap<>();

    private volatile boolean started = false;

    private Thread worker;

    private List<ConnectionClosedEventListener> listeners = new CopyOnWriteArrayList<>();

    BlockingQueue<Object[]> incoming = new LinkedBlockingDeque<>(); // TODO asch OOM is possible, handle that.

    // TODO FIXME asch Or better use com.alipay.sofa.jraft.rpc.RpcUtils.RPC_CLOSURE_EXECUTOR ?
    private ExecutorService defaultExecutor;

    public LocalRpcServer(Endpoint local) {
        this.local = local;
    }

    static boolean connect(LocalRpcClient client, Endpoint srv, boolean createIfAbsent, Consumer<LocalConnection> onCreated) {
        LocalRpcServer locSrv = servers.get(srv);

        if (locSrv == null)
            return false; // Server is not ready.

        LocalConnection conn = locSrv.conns.get(client);

        if (conn == null) {
            if (!createIfAbsent)
                return false;

            conn = new LocalConnection(client, locSrv);

            LocalConnection oldConn = locSrv.conns.putIfAbsent(client, conn);

            if (oldConn == null)
                onCreated.accept(conn);
        }

        return true;
    }

    static void closeConnection(LocalRpcClient client, Endpoint srv) {
        LocalRpcServer locSrv = servers.get(srv);

        if (locSrv == null)
            return;

        LocalConnection conn = locSrv.conns.remove(client);

        if (conn == null)
            return;

        locSrv.listeners.forEach(l -> l.onClosed(client.toString(), conn));
    }

    @Override public void registerConnectionClosedEventListener(ConnectionClosedEventListener listener) {
        if (!listeners.contains(listener))
            listeners.add(listener);
    }

    @Override public void registerProcessor(RpcProcessor<?> processor) {
        processors.put(processor.interest(), processor);
    }

    @Override public int boundPort() {
        return local.getPort();
    }

    @Override public synchronized boolean init(Void opts) {
        if (started)
            return false;

        worker = new Thread(new Runnable() {
            @Override public void run() {
                while(started) {
                    try {
                        Object[] tuple = incoming.take();
                        LocalRpcClient sender = (LocalRpcClient) tuple[0];

                        // Connection is not established, ignore message.
                        LocalConnection conn = conns.get(sender);
                        if (conn == null)
                            continue;

                        Message msg = (Message) tuple[1];
                        CompletableFuture<Object> fut = (CompletableFuture) tuple[2];

                        Class<? extends Message> cls = msg.getClass();
                        RpcProcessor prc = processors.get(cls.getName());

                        // TODO asch cache it.
                        if (prc == null) {
                            for (Class<?> iface : cls.getInterfaces()) {
                                prc = processors.get(iface.getName());

                                if (prc != null)
                                    break;
                            }
                        }

                        RpcProcessor.ExecutorSelector selector = prc.executorSelector();

                        Executor executor = null;

                        if (selector != null) {
                            executor = selector.select(null, msg);
                        }

                        if (executor == null)
                            executor = defaultExecutor;

                        RpcProcessor finalPrc = prc;

                        executor.execute(() -> {
                            finalPrc.handleRequest(new RpcContext() {
                                @Override public void sendResponse(Object responseObj) {
                                    fut.complete(responseObj);
                                }

                                @Override public Connection getConnection() {
                                    return conn;
                                }

                                @Override public String getRemoteAddress() {
                                    return sender.toString();
                                }
                            }, msg);
                        });
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });

        defaultExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("LocalRPCServer-Default-Executor-Thread: " + local.toString()));

        worker.setName("LocalRPCServer-Dispatch-Thread: "  + local.toString());
        worker.start();

        servers.put(local, this);

        started = true;

        return true;
    }

    @Override public synchronized void shutdown() {
        if (!started)
            return;

        started = false;

        worker.interrupt();
        try {
            worker.join();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for RPC server to stop " + local);
        }

        defaultExecutor.shutdownNow();

        try {
            boolean stopped = defaultExecutor.awaitTermination(60_000, TimeUnit.MILLISECONDS);

            if (!stopped) // TODO asch make thread dump.
                LOG.error("Failed to wait for graceful executor shutdown, probably some task is hanging.");
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for RPC server to stop " + local);
        }

        // Close all connections to this server.
        for (LocalRpcClient client : conns.keySet())
            closeConnection(client, local);

        servers.remove(local);
    }
}
