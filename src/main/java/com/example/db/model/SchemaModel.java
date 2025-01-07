package com.example.db.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SchemaModel {
    private int currentId;
    private List<TableColumn> columns = new ArrayList<>();
}
