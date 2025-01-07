package com.example.db.model;

public enum ColumnTypes {
    INT("int"),
    STRING("string");

    private String type;
    ColumnTypes(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }
}
