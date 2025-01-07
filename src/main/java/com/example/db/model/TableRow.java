package com.example.db.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class TableRow {
  private List<TableColumn> cols = new ArrayList<>();
}
