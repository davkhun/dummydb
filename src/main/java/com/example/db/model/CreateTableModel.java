package com.example.db.model;

import lombok.Data;

import java.util.List;

@Data
public class CreateTableModel {
    private String name;
    private List<TableColumn> columns;
}
