package com.si.transformator.services;

import java.io.IOException;
import java.sql.SQLException;

public interface ITransformatorService {

    String TransformModel(String body, Long services_id, Long endpoints_id) throws SQLException, IOException, ClassNotFoundException, CloneNotSupportedException;

    String generateReport(String body, String name);

}
