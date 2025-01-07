package com.example.db.service.operations;

import com.example.db.model.FoundLine;
import com.example.db.model.TableColumn;
import com.example.db.service.helper.DbHelper;
import com.example.db.service.helper.LocalExpressionVisitorAdapter;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateOperation {
  private static Logger log = LoggerFactory.getLogger(CreateOperation.class);

  // TODO: for all schedule operation need to use file logging for errors
  public static void doOperation(String query) throws Exception {
    Update update = (Update) CCJSqlParserUtil.parse(query);
    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
    List<String> tableList = tablesNamesFinder.getTableList(update);
    log.info("Update: " + update);
    // check table is exists
    for (var tbl: tableList) {
      if (!DbHelper.isTableExists(tbl)) {
        // just need to exit, because it is schedule operation
        return;
      }
    }
    String tableName = tableList.get(0);
    var schema = DbHelper.getSchema(tableName);
    // getting columns to update
    // TODO: for simplification and simple update use first element from all updateSets entry
    List<TableColumn> columnsToUpdate = new ArrayList<>();
    for (var updateSet: update.getUpdateSets()) {
      TableColumn col = new TableColumn();
      col.setName(updateSet.getColumn(0).getColumnName());
      col.setValue(updateSet.getValue(0).toString().replace("'",""));
      // get column position
      col.setPosition(schema.getColumns().stream().filter(x->x.getName().equals(col.getName())).findFirst().get().getPosition());
      columnsToUpdate.add(col);
    }
    // evaluate where expression
    Expression whereExpr = update.getWhere();
    if (whereExpr == null) {
      // update all table
      DbHelper.updateTableByLines(tableName, columnsToUpdate, null);
      return;
    }
    // update by where expression
    List<FoundLine> lines = new ArrayList<>();
    net.sf.jsqlparser.expression.Expression expr = CCJSqlParserUtil.parseCondExpression(whereExpr.toString());
    expr.accept(new LocalExpressionVisitorAdapter(lines, null, schema, tableName));
    // update found lines
    if (lines.size() > 0) {
      DbHelper.updateTableByLines(tableName, columnsToUpdate, lines);
    }
  }
}
