package com.here.xyz.httpconnector.util.jobs.datasets.files;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;

@JsonSubTypes({
    @JsonSubTypes.Type(value = GeoJson.class, name = "GeoJson"),
    @JsonSubTypes.Type(value = GeoParquet.class, name = "GeoParquet"),
    @JsonSubTypes.Type(value = Csv.class, name = "Csv")
})
public abstract class FileFormat implements Typed {
}
