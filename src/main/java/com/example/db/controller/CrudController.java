package com.example.db.controller;

import com.example.db.service.CrudService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
