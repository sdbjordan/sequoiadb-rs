package com.sequoiadb.driver;

public class ConnectOptions {
    private String host = "127.0.0.1";
    private int port = 11810;
    private String username;
    private String password;
    private int maxPoolSize = 10;
    private int connectTimeoutMs = 5000;

    public ConnectOptions() {}

    public ConnectOptions(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() { return host; }
    public ConnectOptions setHost(String host) { this.host = host; return this; }

    public int getPort() { return port; }
    public ConnectOptions setPort(int port) { this.port = port; return this; }

    public String getUsername() { return username; }
    public ConnectOptions setUsername(String username) { this.username = username; return this; }

    public String getPassword() { return password; }
    public ConnectOptions setPassword(String password) { this.password = password; return this; }

    public int getMaxPoolSize() { return maxPoolSize; }
    public ConnectOptions setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; return this; }

    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public ConnectOptions setConnectTimeoutMs(int ms) { this.connectTimeoutMs = ms; return this; }
}
