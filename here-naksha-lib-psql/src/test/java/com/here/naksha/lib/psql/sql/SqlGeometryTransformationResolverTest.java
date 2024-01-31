package com.here.naksha.lib.psql.sql;

import com.here.naksha.lib.core.models.storage.transformation.BufferTransformation;
import com.here.naksha.lib.core.models.storage.transformation.GeographyTransformation;
import com.here.naksha.lib.core.models.storage.transformation.GeometryTransformation;
import com.here.naksha.lib.psql.SQL;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.here.naksha.lib.psql.sql.SqlGeometryTransformationResolver.addTransformation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SqlGeometryTransformationResolverTest {

  @Test
  void testNoTransformation() {
    //given
    String variablePlaceholder = "?";

    // when
    SQL sql = addTransformation(null, variablePlaceholder);

    // then
    assertEquals("?", sql.toString());
  }

  @Test
  void testBufferTransformation() {
    //given
    String variablePlaceholder = "?";
    GeometryTransformation bufferTrans = new BufferTransformation(112.21, null, null);

    // when
    SQL sql = addTransformation(bufferTrans, variablePlaceholder);

    // then
    assertEquals(" ST_Buffer(?,112.21,E'') ", sql.toString());
  }

  @Test
  void testGeographyTransformation() {
    //given
    String variablePlaceholder = "?";
    GeometryTransformation geographyTransformation = new GeographyTransformation();

    // when
    SQL sql = addTransformation(geographyTransformation, variablePlaceholder);

    // then
    assertEquals("?::geography ", sql.toString());
  }

  @Test
  void testCombinedTransformation() {
    //given
    String variablePlaceholder = "ST_Force3D(?)";
    GeometryTransformation geographyTransformation = new GeographyTransformation();
    GeometryTransformation combinedTransformation = new BufferTransformation(112.21, "quad_segs=8", geographyTransformation);

    // when
    SQL sql = addTransformation(combinedTransformation, variablePlaceholder);

    // then
    assertEquals(" ST_Buffer(ST_Force3D(?)::geography ,112.21,E'quad_segs=8') ", sql.toString());
  }

  @Test
  void testUnknownTransformation() {
    // given
    String variablePlaceholder = "?";
    GeometryTransformation unknownTransformation = Mockito.mock(GeometryTransformation.class);

    // expect
    assertThrows(UnsupportedOperationException.class, () -> addTransformation(unknownTransformation, variablePlaceholder));
  }
}
