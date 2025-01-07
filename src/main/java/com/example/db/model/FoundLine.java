package com.example.db.model;

import lombok.Data;

// Class for store found line number and line content
@Data
public class FoundLine {
  private Integer lineNumber;
  private String content;
}
