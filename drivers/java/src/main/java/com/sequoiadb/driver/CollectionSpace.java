package com.sequoiadb.driver;

public class CollectionSpace {
    private final SdbClient client;
    private final String name;

    public CollectionSpace(SdbClient client, String name) {
        this.client = client;
        this.name = name;
    }

    public String getName() { return name; }

    public DBCollection getCollection(String clName) {
        return new DBCollection(client, name + "." + clName);
    }

    public DBCollection createCollection(String clName) {
        client.createCollection(name, clName);
        return getCollection(clName);
    }

    public void dropCollection(String clName) {
        client.dropCollection(name, clName);
    }
}
