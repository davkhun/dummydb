package com.example.db.service.helper;

import com.example.db.model.FoundLine;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;

public class FileHelper {

    @SneakyThrows
    public static File getFile(String fileName) {
        ClassLoader classLoader = FileHelper.class.getClassLoader();
        return new File(classLoader.getResource(".").getFile() + "/db/" + fileName);
    }

    @SneakyThrows
    public static void appendLineToFile(File file, String line) {
        PrintWriter printWriter = new PrintWriter(new FileOutputStream(file, true));
        printWriter.append(line).append("\r\n");
        printWriter.close();
    }

    public static boolean isFileExists(String fileName) {
        return FileHelper.getFile(fileName).exists();
    }

    @SneakyThrows
    public static List<String> getSchemaContent(String fileName) {
        return Files.readAllLines(FileHelper.getFile(fileName).toPath());
    }

    @SneakyThrows
    public static List<FoundLine> getTableContent(String fileName) {
        List<FoundLine> result = new ArrayList<>();
        List<String> fileContent = getSchemaContent(fileName);
        int fileLine = 0;
        for (var c : fileContent) {
            FoundLine foundLine = new FoundLine();
            foundLine.setLineNumber(fileLine++);
            foundLine.setContent(c);
            result.add(foundLine);
        }
        return result;
    }

    @SneakyThrows
    public static List<FoundLine> getFileLinesByExpression(String fileName, String delimiter, Integer columnIndex, String searchValue, String operator) {
        List<FoundLine> result = new ArrayList<>();
        String lineResult;
        ClassLoader classLoader = FileHelper.class.getClassLoader();
        try (BufferedReader br = new BufferedReader(new FileReader(classLoader.getResource(".").getFile() + "/db/" + fileName))) {
            String line;
            int lineNumber =0;
            while ((line = br.readLine()) != null) {
                lineResult = searchInLine(line, delimiter, columnIndex, searchValue, operator);
                if (lineResult != null) {
                    FoundLine foundLine = new FoundLine();
                    foundLine.setLineNumber(lineNumber++);
                    foundLine.setContent(lineResult);
                    result.add(foundLine);
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static String searchInLine(String line, String delimiter, Integer columnIndex, String searchValue, String operator) {
        String token = line.split(delimiter)[columnIndex];
        String result = null;
        switch (operator) {
            case "=" -> {
                if (token.equals(searchValue)) {
                    result = line;
                }
            }
            case "!=" -> {
                if (!token.equals(searchValue)) {
                    result = line;
                }
            }
            case "likeLeft" -> {
                if (token.endsWith(searchValue)) {
                    result = line;
                }
            }
            case "likeRight" -> {
                if (token.startsWith(searchValue)) {
                    result = line;
                }
            }
            case "likeFull" -> {
                if (token.contains(searchValue)) {
                    result = line;
                }
            }
            case ">" -> {
                if (TypeHelper.toInt(token) > TypeHelper.toInt(searchValue)) {
                    result = line;
                }
            }
            case "<" -> {
                if (TypeHelper.toInt(token) < TypeHelper.toInt(searchValue)) {
                    result = line;
                }
            }
            case ">=" -> {
                if (TypeHelper.toInt(token) >= TypeHelper.toInt(searchValue)) {
                    result = line;
                }
            }
            case "<=" -> {
                if (TypeHelper.toInt(token) <= TypeHelper.toInt(searchValue)) {
                    result = line;
                }
            }
        }
        return result;
    }

    @SneakyThrows
    public static void updateFileLine(String fileName, int lineIndex, String value) {
        File f = FileHelper.getFile(fileName);
        List<String> content = Files.readAllLines(f.toPath());
        content.set(lineIndex, value);
        Files.write(f.toPath(), content, StandardCharsets.UTF_8);
    }

    @SneakyThrows
    public static void deleteFileLines(String fileName, List<FoundLine> lineIndexes) {
        // remove string from table
        File f = FileHelper.getFile(fileName);
        List<String> content = Files.readAllLines(f.toPath());
        for (var line: lineIndexes) {
            int lineInd = line.getLineNumber();
            // set content to "Removing" to prevent reindexing
            content.set(lineInd, "removing...");
        }
        // remove all found contents
        content = content.stream().filter(x->!x.equals("removing...")).collect(Collectors.toList());
        Files.write(f.toPath(), content, StandardCharsets.UTF_8);
    }

    @SneakyThrows
    public static String getFileLine(String fileName, int lineIndex) {
        File f = FileHelper.getFile(fileName);
        List<String> content = Files.readAllLines(f.toPath());
        return content.get(lineIndex);
    }

    @SneakyThrows
    public static Long getFileLines(String fileName) {
        return Files.lines(FileHelper.getFile(fileName).toPath()).count();
    }

    @SneakyThrows
    public static DirectoryStream<Path> getFilesByMask(String fileMask) {
        ClassLoader classLoader = FileHelper.class.getClassLoader();
        String path = Paths.get(Paths.get(classLoader.getResource(".").toURI()) + "/db").toString();
        return Files.newDirectoryStream(Paths.get(path), fileMask+"*.txt");
    }

    public static void deleteFile(Path fileName) {
        File f = new File(fileName.toString());
        f.delete();
    }

    // for PK
    public static List<Integer> getFileLinesForTable(String fileName, Integer index, String operator)
        throws Exception {
        List<Integer> linesPosition = new ArrayList<>();
        Long linesSize = getFileLines(fileName);
        ClassLoader classLoader = FileHelper.class.getClassLoader();
        String path = Paths.get(Paths.get(classLoader.getResource(".").toURI()) + "/db/" + fileName).toString();
        // when use delete operation, its clear index file, but keep last index sequence. So we need to read first line of file, to get first index value and subs it from index value from method
        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            // but this variant is slow, so we need to keep index table into memory after first select for id and later update it after all CUD operations
           // Integer startIndex = TypeHelper.toInt(getFileLine(fileName, 0).split(",")[0]);
          // index -= startIndex - 1;
          //  if (index < 0) {
           //     return linesPosition;
          //  }
            switch (operator) {
                case "=":
                    linesPosition = lines.skip(index-1).limit(1).map(x->TypeHelper.toInt(x.split(",")[1])).collect(Collectors.toList());
                    break;
                case ">":
                    linesPosition = lines.skip(index).limit(linesSize - index).map(x->TypeHelper.toInt(x.split(",")[1])).collect(Collectors.toList());
                    break;
                case "<":
                    linesPosition = lines.limit(index-1).map(x->TypeHelper.toInt(x.split(",")[1])).collect(Collectors.toList());
                    break;
                case ">=":
                    break;
                case "<=":
                    break;
            }
        }
        return linesPosition;
    }

    public static List<FoundLine> getSpecificLinesFromFile(String fileName, List<Integer> fileLines)
        throws Exception {
        List<FoundLine> result = new ArrayList<>();
        ClassLoader classLoader = FileHelper.class.getClassLoader();
        String path = Paths.get(Paths.get(classLoader.getResource(".").toURI()) + "/db/" + fileName).toString();
        for (var line: fileLines) {
            try (Stream<String> lines = Files.lines(Paths.get(path))) {
                FoundLine foundLine = new FoundLine();
                foundLine.setContent(lines.skip(line).findFirst().get());
                foundLine.setLineNumber(line);
                result.add(foundLine);
            }
        }
        return result;
    }

    @SneakyThrows
    public static void clearFile(String fileName) {
        ClassLoader classLoader = FileHelper.class.getClassLoader();
        String path = Paths.get(Paths.get(classLoader.getResource(".").toURI()) + "/db/" + fileName).toString();
        new PrintWriter(path).close();
    }
}
