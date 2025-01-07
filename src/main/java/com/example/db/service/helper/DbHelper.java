package com.example.db.service.helper;

import com.example.db.model.ColumnTypes;
import com.example.db.model.FoundLine;
import com.example.db.model.SchemaModel;
import com.example.db.model.TableColumn;
import com.example.db.service.operations.CreateOperation;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.naming.NameNotFoundException;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbHelper {
    private static Logger log = LoggerFactory.getLogger(CreateOperation.class);
    public static boolean isTableExists(String tableName) {
        return FileHelper.isFileExists(tableName + ".txt");
    }

    @SneakyThrows
    public static SchemaModel getSchema(String tableName) {
        SchemaModel schema = new SchemaModel();
        List<TableColumn> columns = new ArrayList<>();
        List<String> lines = FileHelper.getSchemaContent(tableName + "_schema.txt");
        schema.setCurrentId(Integer.parseInt(lines.get(0)));
        String plainColumns = lines.get(1);
        Integer position = 1; // because at position 0 we have an _id
        for (String column: plainColumns.split(",")) {
            TableColumn col = new TableColumn();
            String[] tokenColumn = column.split("\\|");
            col.setName(tokenColumn[0]);
            col.setType(ColumnTypes.valueOf(tokenColumn[1].toUpperCase()));
            col.setPosition(position++);
            columns.add(col);
        }
        schema.setColumns(columns);
        return schema;
    }


    // return full table
    @SneakyThrows
    public static List<FoundLine> getTableContent(String tableName) {
        return FileHelper.getTableContent(tableName + ".txt");
    }

    @SneakyThrows
    public static List<FoundLine> selectFromTableByOneExpression(String tableName, String columnName, String searchValue, String operator) {
        SchemaModel schema = getSchema(tableName);
        Integer columnIndex = -1;
        // simplification search index. If search by _id (inner PK), set column index to zero, otherwise, set by i + 1
        if (columnName.equals("_id")) {
            columnIndex = 0;
        }
        else {
            for (int i=0; i< schema.getColumns().size(); i++) {
                if (schema.getColumns().get(i).getName().equals(columnName)) {
                    columnIndex = i+1;
                    break;
                }
            }
        }
        if (columnIndex == -1) {
            throw new NameNotFoundException("Column with name: " + columnName + " not found!");
        }
        return FileHelper.getFileLinesByExpression(tableName + ".txt", ",", columnIndex, searchValue, operator);
    }

    @SneakyThrows
    public static void filterFromTableByOneExpression(String tableName, List<FoundLine> currentResult, String columnName, String searchValue, String operator) {
        SchemaModel schema = getSchema(tableName);
        Integer columnIndex = -1;
        List<String> result = new ArrayList<>();
        // simplification search index. If search by _id (inner PK), set column index to zero, otherwise, set by i + 1
        if (columnName.equals("_id")) {
            columnIndex = 0;
        }
        else {
            for (int i=0; i< schema.getColumns().size(); i++) {
                if (schema.getColumns().get(i).getName().equals(columnName)) {
                    columnIndex = i+1;
                    break;
                }
            }
        }
        List<FoundLine> tmpResult = new ArrayList<>();
        for (int i = 0; i < currentResult.size(); i ++) {
            String res = FileHelper.searchInLine(currentResult.get(i).getContent(), ",", columnIndex, searchValue, operator);
            if (res != null) {
                tmpResult.add(currentResult.get(i));
            }
        }
        currentResult.clear();
        currentResult.addAll(tmpResult);
    }

    public static void insertToTable(String tableName, String line) {
        FileHelper.appendLineToFile(FileHelper.getFile(tableName + ".txt"), line);
    }

    public static void insertToIndexTable(String tableName, String indexName, String line) {
        FileHelper.appendLineToFile(FileHelper.getFile(tableName + "_ind_" + indexName + ".txt"), line);
    }

    public static void updatePkIndexInSchema(String tableName, String newIndexValue) {
        FileHelper.updateFileLine(tableName + "_schema.txt", 0, newIndexValue);
    }

    public static Long getLastLineInTable(String tableName) {
        return FileHelper.getFileLines(tableName + ".txt");
    }

    public static void deleteTable(String tableName) {
        for (Path p: FileHelper.getFilesByMask(tableName)) {
            System.out.println(p.toString());
            FileHelper.deleteFile(p);
        }
    }

    public static void updateTableByLines(String tableName, List<TableColumn> columnsToUpdate, List<FoundLine> lines) {
        List<Integer> fileLines;
        // you can cheat and take a snapshot whole table into memory, update it and save back to file
        // but for large tables you need to update most gently to prevent OOM
        if (lines == null) {
            fileLines = IntStream.range(0, Math.toIntExact(FileHelper.getFileLines(tableName+".txt"))).boxed().collect(Collectors.toList());
        }
        else {
            fileLines = lines.stream().map(FoundLine::getLineNumber).collect(Collectors.toList());
        }
        // update line by line
        for (var line: fileLines) {
            log.info("Updating table: " + tableName + " with line: " + line);
            // get specific line/row
            String lineContent = FileHelper.getFileLine(tableName + ".txt", line);
            // split to values
            String[] tokens = lineContent.split(",");
            // update values by position
            for (var columnToUpdate : columnsToUpdate) {
                tokens[columnToUpdate.getPosition()] = columnToUpdate.getValue();
            }
            // set specific line/row
            FileHelper.updateFileLine(tableName + ".txt", line, String.join(",", tokens));
        }
    }

    public static void deleteTableByLines(String tableName, List<FoundLine> lines) {
        log.info("Deleting from table: " + tableName);
        // delete from table
        FileHelper.deleteFileLines(tableName+".txt", lines);
        // delete from index
        FileHelper.deleteFileLines(tableName+"_ind_id.txt", lines);
    }

    @SneakyThrows
    public static List<FoundLine> getLinesByPk(String tableName, Integer searchValue, String operator) {
        List<Integer> linesPosition = FileHelper.getFileLinesForTable(tableName + "_ind_id.txt", searchValue, operator);
        return FileHelper.getSpecificLinesFromFile(tableName + ".txt", linesPosition);
    }

    public static void truncateTable(String tableName) {
        // truncate table
        FileHelper.clearFile(tableName + ".txt");
        // truncate index
        FileHelper.clearFile(tableName + "_ind_id.txt");
    }

    public static String listToTable(String tableName, List<FoundLine> result, Long timeElapsed, List<String> columns) {
        List<String> content = result.stream().map(FoundLine::getContent).toList();
        String res = "";
        SchemaModel schema = getSchema(tableName);
        res += "Table: " + tableName + "\n";
        res += "---------------------------\n";
        // draw all columns
        if (columns.size() == 0) {
            res += "_id\t";
            for (var c: schema.getColumns()) {
                res += c.getName().toUpperCase() + "\t";
            }
            res += "\n";
            res += "---------------------------\n";
            // draw rows
            for (var r: content) {
                res += r.replace(",","\t") + "\n";
            }
        }
        else {
            // draw specific columns
            for (var c: columns) {
                res += c + "\t";
            }
            res += "\n";
            res += "---------------------------\n";
            // draw rows
            for (var r: content) {
                String[] tokens = r.split(",");
                for (var c: columns) {
                    if (c.equals("_id")) {
                        res += tokens[0] + "\t";
                    }
                    else {
                        Long colIndex = schema.getColumns().stream().takeWhile(x->x.getName().equals(c)).count();
                        res += tokens[Math.toIntExact(colIndex)] + "\t";
                    }
                }
                res += "\n";
            }
        }
        res += "---------------------------\n";
        res += "Time elapsed: " + timeElapsed;
        return res;
    }


}
