package com.example.db.service.operations;

import com.example.db.model.FoundLine;
import com.example.db.service.helper.DbHelper;
import com.example.db.service.helper.LocalExpressionVisitorAdapter;
import com.example.db.service.helper.TypeHelper;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectOperation {
  private static Logger log = LoggerFactory.getLogger(CreateOperation.class);

  // TODO: its just a simple select for 1 table, to use multiple tables in select need to rework getting schemas for all tables and validating and for only one expression
  public static String doOperation(String query) throws Exception {
    StopWatch watch = new StopWatch();
    watch.start();
    Select select = (Select) CCJSqlParserUtil.parse(query);
    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
    List<String> tableList = tablesNamesFinder.getTableList((Statement) select);
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    log.info("Select: " + select);
    // check table is exists
    for (var tbl: tableList) {
      if (!DbHelper.isTableExists(tbl)) {
        return "Table " + tbl + " does not exists!";
      }
    }
    // get table schema
    String tableName = tableList.get(0);
    List<String> columns = new ArrayList<>();
    var schema = DbHelper.getSchema(tableName);
    // check selected columns
    for (var selItem: plainSelect.getSelectItems()) {

      String colName = selItem.getExpression().toString();
      if (colName.equals("*")) {
        break;
      }
      columns.add(colName);
      if (schema.getColumns().stream().filter(x->x.getName().equals(colName)).findAny().isEmpty() && !colName.equals("_id")) {
        return "Column " + colName + " does not exists";
      }
    }
    //start selecting
    List<FoundLine> result = new ArrayList<>();
    List<String> error = new ArrayList<>();
    // if where is null - return whole table
    if (plainSelect.getWhere() == null) {
      result.addAll(DbHelper.getTableContent(tableName));
      watch.stop();
      return result.size() == 0? "Empty result. Time elapsed: " + watch.getTime(): DbHelper.listToTable(tableName, result, watch.getTime(),columns);
    }
    Expression expr = CCJSqlParserUtil.parseCondExpression(plainSelect.getWhere().toString());
    expr.accept(new LocalExpressionVisitorAdapter(result, error, schema, tableName));
    if (error.size() > 0) {
      return error.toString();
    }
    watch.stop();

    return result.size() == 0? "Empty result. Time elapsed: " + watch.getTime(): DbHelper.listToTable(tableName, result, watch.getTime(),columns);
  }
}
