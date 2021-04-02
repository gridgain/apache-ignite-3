package org.apache.ignite.table.distributed.service.command.response;

public class GetResponse<V> {
    private V value;

    public GetResponse(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }
}
