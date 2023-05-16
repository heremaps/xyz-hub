package com.here.naksha.activitylog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.mapcreator.ext.naksha.PsqlDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ActivityLogDBWriter {
    public static void fromActicityLogDBToFeature(PsqlDataSource dataSource) {
        List<String> featureList = new ArrayList<String>();
        try (Connection conn = dataSource.getConnection()) {
            String SQL = "SELECT * FROM activity.\"RnxiONGZ\" LIMIT 10;";
            try (final PreparedStatement stmt = conn.prepareStatement(SQL)) {
                final ResultSet result = stmt.executeQuery();
                while (result.next()) {
                    try {
                        Feature activityLogFeature = XyzSerializable.deserialize(result.getString(1), Feature.class);
                        ActivityLogHandler.fromActivityLogFormat(activityLogFeature);
                        featureList.add(activityLogFeature.serialize());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
