package com.example.db.service;

import com.example.db.model.CreateTableModel;
import com.example.db.model.InsertTableModel;
import com.example.db.service.operations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static com.example.db.service.helper.FileHelper.appendLineToFile;
import static com.example.db.service.helper.FileHelper.getFile;

@Component
public class CrudSchedule {

    private static final Logger log = LoggerFactory.getLogger(CrudSchedule.class);

    private final File journal = getFile("journal.txt");

    private final File backup = getFile("backup_journal.txt");

    @Scheduled(fixedDelay = 1000, initialDelay = 1000)
    public void processJournal() throws Exception {
        // read all lines from journal.txt
        List<String> lines = Files.readAllLines(journal.toPath(), StandardCharsets.UTF_8);
        // clear journal.txt
        new PrintWriter(journal).close();
        // process every query
        boolean isError = false;
        for (String line: lines) {
            try {
                log.info("Process query: " + line);
                if (line.toLowerCase().startsWith("create table")) {
                    CreateTableModel model = CreateOperation.parseQuery(line);
                    CreateOperation.doOperation(model);
                }
                else if (line.toLowerCase().startsWith("insert into")) {
                    InsertTableModel model = InsertOperation.parseQuery(line);
                    InsertOperation.doOperation(model);
                }
                else if (line.toLowerCase().startsWith("drop table")) {
                    DropOperation.doOperation(line);
                }
                else if (line.toLowerCase().startsWith("update")) {
                    UpdateOperation.doOperation(line);
                }
                else if (line.toLowerCase().startsWith("truncate")) {
                    TruncateOperation.doOperation(line);
                }
                else if (line.toLowerCase().startsWith("delete")) {
                    DeleteOperation.doOperation(line);
                }
                else {
                    log.info("Was error in query: " + line);
                    isError = true;
                }
                // if query execution was success, write to backup_journal.txt
                if (!isError) {
                    appendLineToFile(backup, line);
                }
            }
            catch (Exception ex) {
                log.error("Error in query: " + line + ". Message: " + ex.getMessage());
            }
        }
    }
}
