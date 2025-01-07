package com.example.db.service.operations;

import com.example.db.service.helper.DbHelper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropOperation {
    private static Logger log = LoggerFactory.getLogger(CreateOperation.class);
    public static void doOperation(String query) throws JSQLParserException {
        // get table name
        Drop drop = (Drop) CCJSqlParserUtil.parse(query);
        String tableName = drop.getName().getName();
        // delete all tables by mask tableName*.txt
        DbHelper.deleteTable(tableName);
    }
}
