package com.here.naksha.activitylog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.util.IoHelp;
import org.jetbrains.annotations.NotNull;
import com.here.mapcreator.ext.naksha.PsqlDataSource;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ActivityLogDBWriter {
    public static void fromActicityLogDBToFeature(PsqlDataSource dataSource) {
        //List<String> sids = new ArrayList<String>();
        try (Connection conn = dataSource.getConnection()) {
            String SQL = "SELECT * FROM activity.\"RnxiONGZ\" LIMIT 10;";
            try (final PreparedStatement stmt = conn.prepareStatement(SQL)) {
                final ResultSet result = stmt.executeQuery();
                while (result.next()) {
                    //sids.add(result.getString(1));
                    try {
                        //Feature activityLogFeature =  new ObjectMapper().readValue(result.getString(1), Feature.class);
                        Feature activityLogFeature = XyzSerializable.deserialize(result.getString(1), Feature.class);
                        ActivityLogHandler.fromActivityLogFormat(activityLogFeature);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    //final Feature activityLogFeature = XyzSerializable.deserialize(result.getString(1), Feature.class);
                }
                //String cbx = sids.get(0);
                String abc = "";
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
