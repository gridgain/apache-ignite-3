package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Assertions;

public class Server {
    private static final int BASE_PORT = 3344;
    protected static final int BASE_CLIENT_PORT = 10800;
    private static final int BASE_REST_PORT = 10300;

    public void run() throws Exception {
        File workDir = workDir();
        System.out.println("Delete work dir: " + workDir.getAbsolutePath());

        Files.walk(workDir.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);

        workDir.mkdirs();

        Path workPath = workDir.toPath();

        Assertions.assertTrue(workDir.exists());

        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        @Language("HOCON")
        String configTemplate = "ignite {\n"
                + "  \"network\": {\n"
                + "    \"port\":{},\n"
                + "    \"nodeFinder\":{\n"
                + "      \"netClusterNodes\": [ {} ]\n"
                + "    }\n"
                + "  },\n"
                + "  storage.profiles: {"
                + "        " + DEFAULT_STORAGE_PROFILE + ".engine: aipersist, "
                + "        " + DEFAULT_STORAGE_PROFILE + ".sizeBytes: 2073741824 " // Avoid page replacement.
                + "  },\n"
                + "  clientConnector: { port:{} },\n"
                + "  clientConnector.sendServerExceptionStackTraceToClient: true\n"
                + "  rest.port: {},\n"
                + "  raft.fsync = " + fsync() + ",\n"
                + "  system.partitionsLogPath = \"" + logPath() + "\",\n"
                + "  failureHandler.handler: {\n"
                + "      type: \"" + StopNodeOrHaltFailureHandlerConfigurationSchema.TYPE + "\",\n"
                + "      tryStop: true,\n"
                + "      timeoutMillis: 60000,\n" // 1 minute for graceful shutdown
                + "  },\n"
                + "}";

        List<IgniteServer> igniteServers = new ArrayList<>();

        for (int i = 0; i < nodes(); i++) {
            int port = BASE_PORT + i;
            String nodeName = nodeName(port);

            String config = IgniteStringFormatter.format(configTemplate, port, connectNodeAddr,
                    BASE_CLIENT_PORT + i, BASE_REST_PORT + i);

            igniteServers.add(TestIgnitionManager.startWithProductionDefaults(nodeName, config, workPath.resolve(nodeName)));
        }

        String metaStorageNodeName = nodeName(BASE_PORT);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodeNames(metaStorageNodeName)
                .clusterName("cluster")
                .clusterConfiguration(clusterConfiguration())
                .build();

        TestIgnitionManager.init(igniteServers.get(0), initParameters);

        for (IgniteServer node : igniteServers) {
            assertThat(node.waitForInitAsync(), willCompleteSuccessfully());
        }
    }

    protected String clusterConfiguration() {
        return "ignite {}";
    }

    protected int nodes() {
        return 1;
    }

    protected File workDir() throws Exception {
        String userHomeDirectory = System.getProperty("user.home");
        File file = new File(userHomeDirectory);
        File dir = new File(file, "bench_db");
        System.out.println("Work dir: " + dir.getAbsolutePath());
        dir.mkdirs();
        return dir;
    }

    protected String logPath() {
        return "";
    }

    protected boolean fsync() {
        return false;
    }

    private static String nodeName(int port) {
        return "node_" + port;
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server();
        server.run();
    }
}
