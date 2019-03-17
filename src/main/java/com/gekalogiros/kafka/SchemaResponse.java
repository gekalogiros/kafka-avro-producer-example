package com.gekalogiros.kafka;

public class SchemaResponse {

    public String subject;
    public int version;
    public int id;
    public String schema;

    public SchemaResponse() {
    }

    @Override
    public String toString() {
        return "SchemaResponse{" +
                "version=" + version +
                ", id=" + id +
                ", subject='" + subject + '\'' +
                ", schema='" + schema + '\'' +
                '}';
    }
}
