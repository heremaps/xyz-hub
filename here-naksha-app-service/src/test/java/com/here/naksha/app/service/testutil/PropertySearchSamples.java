package com.here.naksha.app.service.testutil;

import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.here.naksha.app.common.TestUtil.urlEncoded;

public class PropertySearchSamples {

  public static Stream<Arguments> queryParams() {
    return Stream.of(
            Pair.of("properties.alone_prop=1", "properties.alone_prop=1"),
            Pair.of("properties.alone_prop=1,2,3,4", "properties.alone_prop=1,2,3,4"),
            Pair.of(
                    "properties.json_prop=" + urlEncoded("{\"arr1\":[1,2],\"arr2\":[]}"),
                    "properties.json_prop={\"arr1\":[1,2],\"arr2\":[]}"
            ),
            Pair.of("properties.long_or=1,2,3,4,4,4,3,2,1,true,false", "properties.long_or=1,2,3,4,4,4,3,2,1,true,false"),
            Pair.of("properties.very.very.very.nested.even.more=1", "properties.very.very.very.nested.even.more=1"),
            Pair.of(
                    // Naksha does not interpret encoded %3E (">" sign) as a gt operation and passes it as properties.prop_> path with "=" operation
                    "properties.prop%3E=value_2,value_22",
                    // Http storege actually sends request with ">" reencoded to %3E, but Wiremock expects decoded string in verify matcher
                    "properties.prop>=value_2,value_22"
            ),
            Pair.of("properties.prop=lte=1", "properties.prop=lte=1"),
            Pair.of("properties.prop=.null", "properties.prop=.null"),
            Pair.of("properties.prop=null", "properties.prop=null"),
            Pair.of("properties.prop!=.null", "properties.prop!=.null"),
            Pair.of("f.id=1", "f.id=1"),
            Pair.of("f.specProp=1", "properties.@ns:com:here:xyz.specProp=1"),
            Pair.of("properties.%40ns%3Acom%3Ahere%3Axyz.specProp=1", "properties.@ns:com:here:xyz.specProp=1"),
            Pair.of("p.propWithShortPrefix=1", "properties.propWithShortPrefix=1"),
            Pair.of("""
                            f.id!=1
                            &properties.prop_2!=value_2,value_22
                            &properties.prop_3=.null,value_33
                            &properties.prop_4!=.null,value_44
                            &properties.prop_5=gte=5.5,55
                            &properties.prop_5_1=cs=%7B%22id%22%3A%22123%22%7D,%5B%7B%22id%22%3A%22123%22%7D%5D
                            &properties.prop_5_2!=%7B%22id%22%3A%22123%22%7D,%7B%22id%22%3A%22456%22%7D,.null
                            &properties.prop_6=lte=6,66
                            &properties.prop_7=gt=7,77
                            &properties.prop_8=lt=8,88
                            &properties.array_1=cs=%40element_1,element_2
                            &properties.prop_10=gte=555,5555
                            &properties.prop_11=lte=666,6666
                            &properties.prop_12=gt=777,7777
                            &properties.prop_13=lt=888,8888
                            &p.prop_14=lt=999,9999
                            &f.prop_15=lt=111,1111
                            &properties.%40ns%3Acom%3Ahere%3Axyz.prop_16=lt=222,2222
                            &properties.@ns:com:here:xyz.tags=cs=%7B%22id%22%3A%22123%22%7D,%5B%7B%22id%22%3A%22123%22%7D%5D,element_4
                            &properties.@ns:com:here:xyz.tags=cs=element_5"""
                            .replace(System.lineSeparator(), "")
                    ,
                    """
                            f.id!=1
                            &properties.prop_2!=value_2,value_22
                            &properties.prop_3=.null,value_33
                            &properties.prop_4!=.null,value_44
                            &properties.prop_5=gte=5.5,55
                            &properties.prop_5_1=cs={\"id\":\"123\"},[{\"id\":\"123\"}],[{\"id\":\"123\"}]
                            &properties.prop_5_2!={\"id\":\"123\"},{\"id\":\"456\"},.null
                            &properties.prop_6=lte=6,66
                            &properties.prop_7=gt=7,77
                            &properties.prop_8=lt=8,88
                            &properties.array_1=cs=@element_1,element_2
                            &properties.prop_10=gte=555,5555
                            &properties.prop_11=lte=666,6666
                            &properties.prop_12=gt=777,7777
                            &properties.prop_13=lt=888,8888
                            &properties.prop_14=lt=999,9999
                            &properties.@ns:com:here:xyz.prop_15=lt=111,1111
                            &properties.@ns:com:here:xyz.prop_16=lt=222,2222
                            """.replace(System.lineSeparator(), "")
            )
    ).map(pair -> {
      RequestPatternBuilder builder = queryToPatternBuilder(pair.getRight());
      return Arguments.of(
              pair.getLeft(),
              builder
      );
    });
  }

  private static @NotNull RequestPatternBuilder queryToPatternBuilder(String query) {
    RequestPatternBuilder builder = new RequestPatternBuilder();
    for (String param : query.split("&")) {
      String[] paramValuePair = param.trim().split("=", 2);
      builder.withQueryParam(paramValuePair[0], equalTo(paramValuePair[1]));
    }
    return builder;
  }
}
