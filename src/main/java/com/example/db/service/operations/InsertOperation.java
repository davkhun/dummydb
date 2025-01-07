package com.example.db.service.operations;

import com.example.db.model.ColumnTypes;
import com.example.db.model.InsertTableModel;
import com.example.db.model.SchemaModel;
import com.example.db.model.TableColumn;
import com.example.db.service.helper.DbHelper;
import com.example.db.service.helper.TypeHelper;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.example.db.service.helper.DbHelper.getSchema;

public class InsertOperation {
    private static Logger log = LoggerFactory.getLogger(CreateOperation.class);

    public static void doOperation(InsertTableModel model) {
        // map model to string
        String line = String.join(",",model.getValues());
        // replace quotes
        line = line.replace("'","");
        // insert data
        DbHelper.insertToTable(model.getTableName(), line);
        // write new value of PK to schema
        DbHelper.updatePkIndexInSchema(model.getTableName(), model.getId().toString());
        // write new PK index value
        Long lastTableLine = DbHelper.getLastLineInTable(model.getTableName()) - 1;
        lastTableLine = lastTableLine < 0? 0: lastTableLine;
        DbHelper.insertToIndexTable(model.getTableName(), "id", model.getId() + "," + lastTableLine);
        // TODO: write other index values
        log.info("--- Insert end ---");
    }

    public static InsertTableModel parseQuery(String query) throws Exception {
        InsertTableModel model = new InsertTableModel();
        Insert insert = (Insert) CCJSqlParserUtil.parse(query);
        // get schema and PK
        SchemaModel schema = getSchema(insert.getTable().getName());
        log.info(schema.toString());
        // check is insert contains enumerating columns
        boolean isCustomColumn = false;
        List<String> customColumns = new ArrayList<>();
        if (insert.getColumns() != null) {
            isCustomColumn = true;
            customColumns = insert.getColumns().stream().map(Column::getColumnName).toList();
            if (!new HashSet<>(schema.getColumns().stream().map(TableColumn::getName).toList())
                    .containsAll(customColumns)) {
                throw new NoSuchFieldException("Columns from query and schema are not equal.");
            }
        }
        List<?> values = insert.getValues().getExpressions();
        // validate values by schema
        // 2 strategy, when use custom columns or all
        if (isCustomColumn) {
            if (values.size() > customColumns.size()) {
                throw new Exception("Values are greater than columns in query");
            }
            List<String> finalCustomColumns = customColumns;
            for (int i = 0; i < customColumns.size(); i++) {
                // get type of column
                int finalI = i;
                ColumnTypes type = schema.getColumns().stream().filter(x->x.getName().equals(finalCustomColumns.get(finalI))).findFirst().get().getType();
                // check if type is INT, otherwise ignore
                if (type.equals(ColumnTypes.INT)) {
                    String checkValue = values.get(i).toString();
                    if (!TypeHelper.isInteger(checkValue)) {
                        throw new NumberFormatException("Value " + checkValue + " in not integer");
                    }
                }
            }
        }
        else {
            // check if values from query greater than in schema
            if (values.size() > schema.getColumns().size()) {
                throw new Exception("Values are greater than columns in query");
            }
            // check type for all values
            for (int i = 0; i < values.size(); i++) {
                ColumnTypes type = schema.getColumns().get(i).getType();
                if (type.equals(ColumnTypes.INT)) {
                    String checkValue = values.get(i).toString();
                    if (!TypeHelper.isInteger(checkValue)) {
                        throw new NumberFormatException("Value " + checkValue + " in not integer");
                    }
                }
            }
        }
        // collect the model
        model.setTableName(insert.getTable().getName());
        model.setId(schema.getCurrentId() + 1);
        List<String> insertValues = new ArrayList<>();
        insertValues.add(model.getId().toString());
        insertValues.addAll(values.stream().map(Object::toString).toList());
        model.setValues(insertValues);
        return model;
    }
}
