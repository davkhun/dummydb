package com.example.db.controller;

import com.example.db.service.CrudService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping(path="/")
public class CrudController {

    @Autowired
    CrudService crudService;

    @PostMapping(path = "/query")
    public String query(@RequestBody String request) throws Exception {
        return crudService.proceedQuery(request);
    }

}
