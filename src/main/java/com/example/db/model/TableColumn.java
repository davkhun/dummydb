package com.example.db.model;

import lombok.Data;

@Data
public class TableColumn {
    private Integer position;
    private String name;
    private ColumnTypes type;
    private String value;

    public String toString() {
        return this.name + "|" + this.type;
    }
}
