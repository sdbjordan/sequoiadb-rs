package com.sequoiadb.driver;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Helper to build and start the Rust sequoiadb-server for integration tests.
 */
public class TestServer {
    private static final String PROJECT_ROOT;
    private static final String SERVER_BINARY;

    static {
        // Resolve project root: drivers/java/../../..
        Path javaDir = Path.of(System.getProperty("user.dir"));
        Path root;
        if (javaDir.endsWith("java")) {
            root = javaDir.getParent().getParent();
        } else if (javaDir.endsWith("drivers")) {
            root = javaDir.getParent();
        } else {
            // Assume we're at sequoiadb-rs root or drivers/java
            root = javaDir;
            if (Files.exists(root.resolve("drivers"))) {
                // at project root
            } else if (Files.exists(root.resolve("pom.xml"))) {
                root = root.getParent().getParent();
            }
        }
        PROJECT_ROOT = root.toAbsolutePath().toString();
        SERVER_BINARY = PROJECT_ROOT + "/target/release/sequoiadb";
    }

    private Process process;
    private int port;

    public int getPort() { return port; }

    public void start() throws Exception {
        // Build
        ProcessBuilder buildPb = new ProcessBuilder("cargo", "build", "--release")
                .directory(new File(PROJECT_ROOT))
                .redirectErrorStream(true);
        Process buildProcess = buildPb.start();
        buildProcess.getInputStream().transferTo(System.out);
        int exitCode = buildProcess.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("cargo build failed with exit code " + exitCode);
        }

        port = findFreePort();
        String dataDir = PROJECT_ROOT + "/target/test-data-java-" + port;
        new File(dataDir).mkdirs();

        ProcessBuilder serverPb = new ProcessBuilder(SERVER_BINARY, "-p", String.valueOf(port), "--db-path", dataDir)
                .directory(new File(PROJECT_ROOT))
                .redirectErrorStream(true);
        process = serverPb.start();

        waitForPort(port, 10000);
    }

    public void stop() {
        if (process != null) {
            process.destroyForcibly();
            try { process.waitFor(); } catch (InterruptedException ignored) {}
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    private static void waitForPort(int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                new java.net.Socket("127.0.0.1", port).close();
                return;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        throw new RuntimeException("Server did not start on port " + port);
    }
}
