package com.example.db.service.operations;

import com.example.db.model.ColumnTypes;
import com.example.db.model.TableColumn;
import com.example.db.model.CreateTableModel;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.example.db.service.helper.FileHelper.getFile;

public class CreateOperation {

    private static Logger log = LoggerFactory.getLogger(CreateOperation.class);
    public static void doOperation(CreateTableModel model) throws IOException {
        ClassLoader classLoader = CreateOperation.class.getClassLoader();
        File file = new File(classLoader.getResource(".").getFile() + "/db/" + model.getName() + ".txt");
        if (file.createNewFile()) {
            // make schema (current index value, list of columns divided by comma)
            file = getFile(model.getName() + "_schema.txt");
            PrintWriter printWriter = new PrintWriter(new FileOutputStream(file));
            printWriter.println(0);
            String columns = model.getColumns().stream().map(TableColumn::toString).collect(Collectors.joining(","));
            printWriter.println(columns);
            printWriter.close();
            // make pk index
            getFile(model.getName() + "_ind_id.txt");
            log.info("Table " + model.getName() + " created successfully");
        }
        else {
            throw new FileAlreadyExistsException("File " + model.getName() + ".txt already exists");
        }
    }

    public static CreateTableModel parseQuery(String query) throws JSQLParserException {
        CreateTableModel model = new CreateTableModel();
        List<TableColumn> createTableColumnList = new ArrayList<>();
        CreateTable createTable = (CreateTable) CCJSqlParserUtil.parse(query);
        for (ColumnDefinition columnDefinition: createTable.getColumnDefinitions()) {
            TableColumn tableColumn = new TableColumn();
            tableColumn.setType(ColumnTypes.valueOf(columnDefinition.getColDataType().getDataType().toUpperCase()));
            tableColumn.setName(columnDefinition.getColumnName());
            createTableColumnList.add(tableColumn);
        }
        // collect model
        model.setName(createTable.getTable().getName());
        model.setColumns(createTableColumnList);
        return model;
    }
}
