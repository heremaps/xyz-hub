package com.here.naksha.app.service.testutil;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.common.Gzip;

import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

public class GzipUtil {

  public static void stubOkGzipEncoded(MappingBuilder mappingToStub, String bodyToEncode) {
    stubFor(mappingToStub.willReturn(
            ok()
                    .withBody(Gzip.gzip(bodyToEncode))
                    .withHeader("Content-Encoding", "gzip")
    ));
  }
}
