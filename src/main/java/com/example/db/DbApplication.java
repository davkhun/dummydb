package com.example.db;

import jakarta.annotation.PostConstruct;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;

@SpringBootApplication
@EnableScheduling
public class DbApplication {

	public static void main(String[] args) {
		SpringApplication.run(DbApplication.class, args);
	}

	@PostConstruct
	@SneakyThrows
	public void init() {
		// create journal file if it does not exist
		ClassLoader classLoader = getClass().getClassLoader();
		Files.createDirectories(Paths.get(Paths.get(classLoader.getResource(".").toURI()) + "/db"));
		File file = new File(classLoader.getResource(".").getFile() + "/db/journal.txt");
		file.createNewFile();
		file = new File(classLoader.getResource(".").getFile() + "/db/backup_journal.txt");
		file.createNewFile();
	}

}
