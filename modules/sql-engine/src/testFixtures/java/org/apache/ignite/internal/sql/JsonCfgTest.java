package org.apache.ignite.internal.sql;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.junit.jupiter.api.Test;

public class JsonCfgTest extends ClusterPerClassIntegrationTest  {

    /**
     * Returns node bootstrap config template.
     *
     * @return Node bootstrap config template.
     */
    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return "{ \"ignite\": {\n"
                + "  \"network\": {\n"
                + "    \"port\": {},\n"
                + "    \"nodeFinder\": { \"netClusterNodes\": [ {} ] } \n"
                + "  },\n"
                + "  \"storage\": { \"profiles\": {"
                + "        \"" + DEFAULT_TEST_PROFILE_NAME +      "\": { \"engine\": \"test\" } , "
                + "        \"" + DEFAULT_AIPERSIST_PROFILE_NAME + "\": { \"engine\": \"aipersist\" }, "
                + "        \"" + DEFAULT_AIMEM_PROFILE_NAME +     "\": { \"engine\": \"aimem\" }, "
                + "        \"" + DEFAULT_ROCKSDB_PROFILE_NAME +   "\": { \"engine\": \"rocksdb\" }"
                + "  } },\n"
                + "  \"clientConnector\": { \"port\": {}, \"sendServerExceptionStackTraceToClient\": true },\n"
                + "  \"rest\": { \"port\": {} },\n"
                + "  \"compute\": { \"threadPoolSize\": 1 },\n"
                + "  \"failureHandler\": { \"dumpThreadsOnFailure\": false }\n"
                + "} }";
    }

    @Test
    public void ddd() {

    }
}
