package com.example.db.service.operations;

import com.example.db.model.FoundLine;
import com.example.db.service.helper.DbHelper;
import com.example.db.service.helper.LocalExpressionVisitorAdapter;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteOperation {
  private static Logger log = LoggerFactory.getLogger(CreateOperation.class);

  public static void doOperation(String query) throws Exception {
    Delete delete = (Delete) CCJSqlParserUtil.parse(query);
    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
    List<String> tableList = tablesNamesFinder.getTableList(delete);
    log.info("Delete: " + delete);
    // check table is exists
    for (var tbl: tableList) {
      if (!DbHelper.isTableExists(tbl)) {
        log.error("Table " + tbl + " does not exists!");
        return;
      }
    }
    String tableName = tableList.get(0);
    // full delete
    if (delete.getWhere() == null) {
      // index position keep its value, just deleting data
      DbHelper.truncateTable(tableName);
      return;
    }
    // update by where expression
    List<FoundLine> lines = new ArrayList<>();
    net.sf.jsqlparser.expression.Expression expr = CCJSqlParserUtil.parseCondExpression(delete.getWhere().toString());
    var schema = DbHelper.getSchema(tableName);
    expr.accept(new LocalExpressionVisitorAdapter(lines, null, schema, tableName));
    // update found lines
    if (lines.size() > 0) {
      DbHelper.deleteTableByLines(tableName, lines);
    }
  }
}
