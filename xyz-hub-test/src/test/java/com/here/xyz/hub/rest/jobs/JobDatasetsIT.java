package com.here.xyz.hub.rest.jobs;

import static org.junit.Assert.assertThrows;

import org.junit.Test;

import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Point;

public class JobDatasetsIT {

// uncommeted validation s.  https://here-technologies.atlassian.net/browse/DS-657   
//    @Test
    public void createInvalidSpatialFilter() {
        
        assertThrows(InvalidGeometryException.class, 
                     () -> new SpatialFilter().withGeometry(new Point().withCoordinates(new PointCoordinates(399,399))));

    }


}
