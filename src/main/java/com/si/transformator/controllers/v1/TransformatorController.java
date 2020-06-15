package com.si.transformator.controllers.v1;

import com.si.transformator.services.ITransformatorService;
import com.si.transformator.utils.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.sql.SQLException;

@Controller
@RequestMapping(Constants.FILTER_BASE_URL)
public class TransformatorController {

    private final ITransformatorService transformatorService;

    @Autowired
    public TransformatorController(ITransformatorService transformatorService) {
        this.transformatorService = transformatorService;
    }

    @RequestMapping(method = {RequestMethod.POST})
    public ResponseEntity<String> filterRequest(@RequestBody String body, @RequestParam(name = "S") Long services_id, @RequestParam(name = "E") Long endpoints_id) throws SQLException, IOException, ClassNotFoundException, CloneNotSupportedException {

        return new ResponseEntity(transformatorService.TransformModel(body, services_id, endpoints_id), HttpStatus.OK);
    }

    @RequestMapping(method = {RequestMethod.POST}, value = "report")
    public ResponseEntity<String> generateReport(@RequestBody String body, @RequestParam(name = "name") String name) throws SQLException, IOException, ClassNotFoundException, CloneNotSupportedException {

        return new ResponseEntity(transformatorService.generateReport(body, name), HttpStatus.OK);
    }
}
