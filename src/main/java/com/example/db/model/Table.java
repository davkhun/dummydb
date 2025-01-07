package com.example.db.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class Table {
  private List<TableRow> rows = new ArrayList<>();

}
