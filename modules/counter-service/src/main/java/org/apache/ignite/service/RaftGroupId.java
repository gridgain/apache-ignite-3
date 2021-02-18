package org.apache.ignite.service;

public class RaftGroupId {

    private final GroupType srvType;

    private final int idx;

    public RaftGroupId(
        GroupType srvType, int idx) {
        this.srvType = srvType;
        this.idx = idx;
    }

    /**
     * @return Index.
     */
    public int index() {
        return idx;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RaftGroupId id = (RaftGroupId)o;

        if (idx != id.idx)
            return false;
        return srvType == id.srvType;
    }

    @Override public int hashCode() {
        int result = srvType != null ? srvType.hashCode() : 0;
        result = 31 * result + idx;
        return result;
    }

    public enum GroupType {
        PARTITIONED,
        METASTORAGE,
        COUNTER;
    }
}
