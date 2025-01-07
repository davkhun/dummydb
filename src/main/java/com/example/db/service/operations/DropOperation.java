package com.example.db.service.operations;

import com.example.db.service.helper.DbHelper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.drop.Drop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateOperation.class);
    public static void doOperation(String query) throws JSQLParserException {
        // get table name
        Drop drop = (Drop) CCJSqlParserUtil.parse(query);
        String tableName = drop.getName().getName();
        log.info("Drop table: " + tableName);
        // delete all tables by mask tableName*.txt
        DbHelper.deleteTable(tableName);
    }
}
