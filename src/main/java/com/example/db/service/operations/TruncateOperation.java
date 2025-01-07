package com.example.db.service.operations;

import com.example.db.service.helper.DbHelper;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TruncateOperation {
  private static Logger log = LoggerFactory.getLogger(CreateOperation.class);

  public static void doOperation(String query) throws Exception {
    Truncate truncate = (Truncate) CCJSqlParserUtil.parse(query);
    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
    List<String> tableList = tablesNamesFinder.getTableList((Statement) truncate);
    log.info("Truncate: " + truncate);
    // check table is exists
    for (var tbl: tableList) {
      if (!DbHelper.isTableExists(tbl)) {
        log.error("Table " + tbl + " does not exists!");
        return;
      }
    }
    // clear table file
    DbHelper.truncateTable(tableList.get(0));
    // set PK index to zero
    DbHelper.updatePkIndexInSchema(tableList.get(0), "0");
  }
}
