package com.example.db.service;

import com.example.db.service.operations.SelectOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.example.db.service.helper.FileHelper.getFile;

@Service
public class CrudServiceImpl implements CrudService {

    private static Logger log = LoggerFactory.getLogger(CrudService.class);
    @Autowired
    ResourceLoader resourceLoader;

    @Override
    public String proceedQuery(String query) throws Exception {
        // replace all new lines to spaces
        String pureQuery = query.replaceAll("[\\t\\n\\r]+"," ");
        // for create, update, delete - write journal, select - execute immediately
        if (pureQuery.toLowerCase().matches("^select.*$")) {
            return SelectOperation.doOperation(pureQuery);
        }
        else if (pureQuery.toLowerCase().matches("^(create|update|delete|insert|alter|drop|truncate).*$")){
            writeJournal(pureQuery);

        }
        else {
            throw new Exception("Bad query: " + pureQuery);
        }
        log.info(pureQuery);
        return "OK!";
    }

    private void writeJournal(String query) throws Exception {

        // validate query
        if (!isValidRequest(query)) {
            throw new Exception("Query is not valid");
        }
        List<String> queries = new ArrayList<>();
        // for insert query there may be many inserts, which need to transform to list of insert queries. Otherwise, array list contain just one query
        if (query.toLowerCase().startsWith("insert into")) {
            String firstPart = query.substring(0, query.lastIndexOf("values")+6);
            String lastPart = query.substring(query.lastIndexOf("values")+6);
            Pattern insertPattern = Pattern.compile("(?=\\()(?:(?=.*?\\((?!.*?\\1)(.*\\)(?!.*\\2).*))(?=.*?\\)(?!.*?\\2)(.*)).)+?.*?(?=\\1)[^(]*(?=\\2$)");
            Matcher insertMatcher = insertPattern.matcher(lastPart);
            Integer i = 0;
            // TODO: too slow on large inserts (over 1k)
            while (insertMatcher.find()) {
                queries.add(firstPart + " " + insertMatcher.group());
            }
        }
        else {
            queries.add(query);
        }
        // write list of queries to end of journal file
        File journalFile = getFile("journal.txt");
        PrintWriter journalPrintWriter = new PrintWriter(new FileOutputStream(journalFile, true));
        for (String line: queries) {
            journalPrintWriter.append(line + "\r\n");
        }
        journalPrintWriter.close();
    }

    private boolean isValidRequest(String query) {
        return true;
    }

    private void updatePk(String tableName) {
        // read all lines from table file
        // create index model by line number and index
        // sort by index
        // write pk file
    }

}