package com.example.db.model;

import lombok.Data;

import java.util.List;

@Data
public class InsertTableModel {
    private String tableName;
    private Integer id;
    private List<String> values;
}
