package com.here.xyz.hub.params;

import static com.here.xyz.events.QueryDelimiter.COMMA;
import static com.here.xyz.events.QueryParameterType.BOOLEAN;
import static com.here.xyz.events.QueryParameterType.DOUBLE;
import static com.here.xyz.events.QueryParameterType.LONG;
import static com.here.xyz.events.QueryParameterType.STRING;

import com.amazonaws.util.StringUtils;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.QueryDelimiter;
import com.here.xyz.events.QueryOperator;
import com.here.xyz.events.QueryParameter;
import com.here.xyz.events.QueryParameterType;
import com.here.xyz.events.QueryParameters;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.exceptions.ParameterFormatError;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.responses.XyzError;
import io.vertx.ext.web.RoutingContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Special implementation for query parameters.
 */
public class XyzHubQueryParameters extends QueryParameters {

  static final String ACCESS_TOKEN = "access_token";
  static final String ID = "id";
  static final String ADD_TAGS = "addTags";
  static final String REMOVE_TAGS = "removeTags";
  static final String TAGS = "tags";
  static final String SELECTION = "selection";
  static final String SORT = "sort";
  static final String PART = "part";
  static final String IF_EXISTS = "e";
  static final String IF_NOT_EXISTS = "ne";
  static final String TRANSACTIONAL = "transactional";
  static final String CONFLICT_RESOLUTION = "cr";
  static final String PREFIX_ID = "prefixId";
  static final String CLIP = "clip";
  static final String SKIP_CACHE = "skipCache";
  static final String CLUSTERING = "clustering";
  static final String TWEAKS = "tweaks";
  static final String LIMIT = "limit";
  static final String WEST = "west";
  static final String NORTH = "north";
  static final String EAST = "east";
  static final String SOUTH = "south";
  static final String MARGIN = "margin";
  static final String EPSG = "epsg";
  static final String HANDLE = "handle";
  static final String INCLUDE_RIGHTS = "includeRights";
  static final String INCLUDE_CONNECTORS = "includeConnectors";
  static final String OWNER = "owner";
  static final String LAT = "lat";
  static final String LON = "lon";
  static final String RADIUS = "radius";
  static final String REF_SPACE_ID = "refSpaceId";
  static final String REF_FEATURE_ID = "refFeatureId";
  static final String H3_INDEX = "h3Index";
  static final String CONTENT_UPDATED_AT = "contentUpdatedAt";
  static final String CONTEXT = "context";

  static final String CLUSTERING_PARAM_RESOLUTION = "resolution";
  static final String CLUSTERING_PARAM_RESOLUTION_RELATIVE = "relativeResolution";
  static final String CLUSTERING_PARAM_RESOLUTION_ABSOLUTE = "absoluteResolution";
  static final String CLUSTERING_PARAM_NOBUFFER = "noBuffer";
  static final String CLUSTERING_PARAM_PROPERTY = "property";
  static final String CLUSTERING_PARAM_POINTMODE = "pointmode";
  static final String CLUSTERING_PARAM_SINGLECOORD = "singlecoord";
  static final String CLUSTERING_PARAM_COUNTMODE = "countmode";
  static final String CLUSTERING_PARAM_SAMPLING = "sampling";

  static final String TWEAKS_PARAM_STRENGTH = "strength";
  static final String TWEAKS_PARAM_ALGORITHM = "algorithm";
  static final String TWEAKS_PARAM_DEFAULT_SELECTION = "defaultselection";
  static final String TWEAKS_PARAM_SAMPLINGTHRESHOLD = "samplingthreshold";

  static final String FORCE_2D = "force2D";
  static final String OPTIM_MODE = "mode";
  static final String OPTIM_VIZSAMPLING = "vizSampling";

  static final String VERSION = "version";
  static final String START_VERSION = "startVersion";
  static final String END_VERSION = "endVersion";
  static final String PAGE_TOKEN = "pageToken";

  static final String REVISION = "revision";
  static final String AUTHOR = "author";
  static final String SUBSCRIPTION_SOURCE = "source";

  private static final Map<@NotNull String, @NotNull QueryParameterType> fixedParameterTypes = new HashMap<>();

  static {
    fixedParameterTypes.put(ID, STRING);
    fixedParameterTypes.put(ADD_TAGS, STRING);
    fixedParameterTypes.put(REMOVE_TAGS, STRING);
    fixedParameterTypes.put(TAGS, STRING);
    fixedParameterTypes.put(HANDLE, STRING);
    fixedParameterTypes.put(REF_SPACE_ID, STRING);
    fixedParameterTypes.put(REF_FEATURE_ID, STRING);
    fixedParameterTypes.put(OWNER, STRING);

    fixedParameterTypes.put(INCLUDE_RIGHTS, BOOLEAN);
    fixedParameterTypes.put(INCLUDE_CONNECTORS, BOOLEAN);

    fixedParameterTypes.put(LON, DOUBLE);
    fixedParameterTypes.put(EAST, DOUBLE);
    fixedParameterTypes.put(WEST, DOUBLE);
    fixedParameterTypes.put(LAT, DOUBLE);
    fixedParameterTypes.put(NORTH, DOUBLE);
    fixedParameterTypes.put(SOUTH, DOUBLE);
    fixedParameterTypes.put(RADIUS, DOUBLE);
    fixedParameterTypes.put(LIMIT, LONG);
    fixedParameterTypes.put(MARGIN, LONG);
  }

  @Override
  protected @NotNull QueryParameterType typeOfValue(@NotNull String key, int index) {
    final QueryParameterType parameterType = fixedParameterTypes.get(key);
    return parameterType != null ? parameterType : QueryParameterType.ANY;
  }

  @Override
  protected boolean stopOnDelimiter(final @Nullable String key, @Nullable QueryOperator op,
      final int index, final int number,
      final boolean quoted, final @NotNull QueryDelimiter delimiter, final @NotNull StringBuilder sb) {
    if (key != null && op != null) {
      // If value.
      if (TAGS.equals(key)) {
        // TODO: Add separators
      }
    }
    // Expand all keys, and the values of some keys.
    if (key == null || (op != null && (SELECTION.equals(key) || SORT.equals(key)))) {
      // For example "&:id=foo" or "&selection=:id"
      if (sb.length() == 0) {
        sb.append(':');
        return false;
      }
      // For example "&p:name=foo" or "&selection=p:name".
      if (equals("p", sb)) {
        sb.setLength(0);
        sb.append(":properties.");
        return false;
      }
      // The fist allows basically to use "p:xyz:{param}", which will be expanded above to "properties.xyz".
      // The second allows an extreme short form of "xyz:createdAt".
      if (equals(":properties.xyz", sb) || equals("xyz", sb)) {
        sb.setLength(0);
        sb.append(":properties.@ns:com.here.xyz.");
        return false;
      }
    }
    // Default implementation handles comma as value separator, the default split of keys, operations and alike.
    return super.stopOnDelimiter(key, op, index, number, quoted, delimiter, sb);
  }

  @Override
  protected boolean addDelimiter(@NotNull QueryParameter parameter, @NotNull QueryDelimiter delimiter) {
    if (TAGS.equals(parameter.getKey())) {
      // TODO: Add separators
    }
    return false;
  }

  @Override
  protected void validateAndSetDefault(@NotNull QueryParameter parameter) {
  }

  /**
   * Creates a new query parameters map.
   *
   * @param queryString the query string without the question mark, for example {@code "foo=x&bar=y"}.
   * @throws XyzErrorException If parsing the query string failed.
   */
  public XyzHubQueryParameters(@Nullable String queryString) throws XyzErrorException {
    super(queryString);
  }

  enum AdditionalParamsType {
    CLUSTERING("clustering"),
    TWEAKS("tweaks");

    AdditionalParamsType(@NotNull String type) {
      this.type = type;
    }

    public static @Nullable AdditionalParamsType of(@Nullable CharSequence text) {
      if (text == null) {
        return null;
      }
      final String s = text.toString();
      for (final AdditionalParamsType additionalParamsType : AdditionalParamsType.values()) {
        if (additionalParamsType.type.equals(s)) {
          return additionalParamsType;
        }
      }
      return null;
    }

    public final @NotNull String type;

    @Override
    public @NotNull String toString() {
      return type;
    }
  }

  /**
   * Returns the list of IDs.
   *
   * @return The list or IDs; {@code null} if no IDs given.
   */
  public @Nullable List<@NotNull String> getIds() {
    //noinspection unchecked,rawtypes
    return (List<@NotNull String>) (List) join(ID);
  }

  /**
   * Returns the first ID from the list of IDs.
   *
   * @return The list or IDs; {@code null} if no IDs given.
   */
  public @Nullable String getId() {
    final List<@NotNull String> ids = getIds();
    if (ids == null || ids.size() == 0) {
      return null;
    }
    return ids.get(0);
  }

  private TagsQuery __tagsQuery;

  /**
   * Returns the parsed “tags” parameter.
   *
   * @return The parsed tags; {@code null} if no tags given.
   */
  public @NotNull TagsQuery getTags() {
    if (__tagsQuery != null) {
      return __tagsQuery;
    }
    final QueryParameter tags = join(TAGS);
    return __tagsQuery = (tags != null ? TagsQuery.fromQueryParameter(tags.asStringList()) : new TagsQuery());
  }

  /**
   * Returns the property selection.
   *
   * @param key the key.
   * @return The property selection; {@code null} if all properties selected.
   */
  private @Nullable List<@NotNull String> getSelection(@NotNull String key) {
    final List<@NotNull String> rawSelection = getAll(key);
    if (rawSelection == null || rawSelection.size() == 0 ||
        (rawSelection.size() == 1 && "*".equals(rawSelection.get(0)))) {
      return null;
    }
    final List<@NotNull String> selection = new ArrayList<>(rawSelection.size());
    for (final String raw : rawSelection) {
      final String value = keyToPath(raw);
      if (value != null) {
        selection.add(value);
      }
    }
    return selection;
  }

  /**
   * Returns the property selection, so the properties that should be part of the response.
   *
   * @return The property selection; {@code null} if all properties should be returned.
   */
  public @Nullable List<@NotNull String> getSelection() {
    return getSelection(SELECTION);
  }

  /**
   * Returns the properties above which to order.
   *
   * @return The properties above which to order; {@code null} if no order selected.
   */
  public @Nullable List<@NotNull String> getOrder() {
    return getSelection(SORT);
  }

  // ?
  public @NotNull Integer @Nullable [] getPart() {
    final List<@NotNull String> all = getAll(PART);
    int part, total;
    if (all != null && all.size() >= 1 && all.size() <= 2) {
      try {
        part = Integer.parseUnsignedInt(all.get(0));
        total = (all.size() > 1 ? Integer.parseUnsignedInt(all.get(1)) : /* -1 */ 1); // -1 to use n-handle modus
        return (part == 0 || total == 0) ? null : new Integer[]{Math.min(part, total), Math.max(part, total)};
      } catch (NumberFormatException ignore) {
      }
    }
    return null;
  }

  public int getRadius() {
    return getInteger(RADIUS, 0);
  }

  public @Nullable String getRefFeatureId() {
    return getString(REF_FEATURE_ID);
  }

  public @Nullable String getRefSpaceId() {
    return getString(REF_SPACE_ID);
  }

  public @Nullable String getH3Index() {
    return getString(H3_INDEX);
  }

  /**
   * Returns the point encoded in the query parameters "&lat" and "&lon".
   *
   * @return the point parsed from query parameters; {@code null} if no point given.
   * @throws XyzErrorException If the parameters are illegal, for example only "&lat" provided.
   */
  public @Nullable Point getPoint() throws XyzErrorException {
    final Double lat;
    try {
      lat = getDouble(LAT);
    } catch (ParameterFormatError e) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Failed to parse 'lon': " + e.getMessage());
    }
    final Double lon;
    try {
      lon = getDouble(LON);
    } catch (ParameterFormatError e) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Failed to parse 'lat': " + e.getMessage());
    }
    if (lat == null && lon == null) {
      return null;
    }
    if (lat == null) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "'lon' given, but 'lat' missing");
    }
    if (lon == null) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "'lat' given, but 'lon' missing");
    }

    if (lat < -90d || lat > 90d) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT,
          "Invalid 'lat' query parameter, must be a WGS'84 longitude in decimal degree (so a value between -90 and +90).");
    }
    if (lon < -180d || lon > 180d) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT,
          "Invalid 'lon' query parameter, must be a WGS'84 longitude in decimal degree (so a value between -180 and +180).");
    }
    return new Point().withCoordinates(new PointCoordinates(lon, lat));
  }

  /**
   * Retures the parsed query parameter for space
   */
  static PropertiesQuery getSpacePropertiesQuery(RoutingContext context, String param) {
    PropertiesQuery propertyQuery = context.get("propertyQuery");
    if (propertyQuery == null) {
      propertyQuery = parsePropertiesQuery(context.request().query(), param, true);
      context.put("propertyQuery", propertyQuery);
    }
    return propertyQuery;
  }

  /**
   * Returns the parsed tags parameter
   */
  static PropertiesQuery getPropertiesQuery(RoutingContext context) {
    PropertiesQuery propertyQuery = context.get("propertyQuery");
    if (propertyQuery == null) {
      propertyQuery = parsePropertiesQuery(context.request().query(), "", false);
      context.put("propertyQuery", propertyQuery);
    }
    return propertyQuery;
  }

  /**
   * Returns the first property found in the query string in the format of key-operator-value(s)
   *
   * @param query      the query part in the url without the '?' symbol
   * @param key        the property to be searched
   * @param multiValue when true, checks for comma separated values, otherwise return the first value found
   * @return null in case none is found
   */
  static PropertyQuery getPropertyQuery(String query, String key, boolean multiValue) {
    if (StringUtils.isNullOrEmpty(query) || StringUtils.isNullOrEmpty(key)) {
      return null;
    }

    int startIndex;
    if ((startIndex = query.indexOf(key)) != -1) {
      String opValue = query.substring(startIndex + key.length()); // e.g. =eq=head
      String operation = shortOperators
          .stream()
          .sorted(Comparator.comparingInt(k -> k.length() * -1)) // reverse a sorted list because u want to get the longer ops first.
          .filter(opValue::startsWith) // e.g. in case of key=eq=val, 2 ops will be filtered in: '=eq=' and '='.
          .findFirst() // The reversed sort plus the findFirst makes sure the =eq= is the one you are looking for.
          .orElse(null); // e.g. anything different from the allowed operators

      if (operation == null) {
        return null;
      }

      String value = opValue.substring(operation.length()).split("&")[0];
      List<Object> values = multiValue
          ? Arrays.asList(value.split(","))
          : Collections.singletonList(value.split(",")[0]);

      return new PropertyQuery()
          .withKey(key)
          .withOperation(operators.get(operation))
          .withValues(values);
    }

    return null;
  }

  protected static PropertiesQuery parsePropertiesQuery(String query, String property, boolean spaceProperties) {
    if (query == null || query.length() == 0) {
      return null;
    }

    PropertyQueryList pql = new PropertyQueryList();
    Stream.of(query.split("&"))
        .filter(k -> k.startsWith("p.") || k.startsWith("f.") || spaceProperties)
        .forEach(keyValuePair -> {
          PropertyQuery propertyQuery = new PropertyQuery();

          String operatorComma = "-#:comma:#-";
          try {
            keyValuePair = keyValuePair.replaceAll(",", operatorComma);
            keyValuePair = URLDecoder.decode(keyValuePair, "utf-8");
          } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
          }

          int position = 0;
          String op = null;

          /** store "main" operator. Needed for such cases foo=bar-->test*/
          for (String shortOperator : shortOperators) {
            int currentPositionOfOp = keyValuePair.indexOf(shortOperator);
            if (currentPositionOfOp != -1) {
              if (
                // feature properties query
                  (!spaceProperties && (op == null || currentPositionOfOp < position || (currentPositionOfOp == position
                      && op.length() < shortOperator.length()))) ||
                      // space properties query
                      (keyValuePair.substring(0, currentPositionOfOp).equals(property) && spaceProperties && (op == null
                          || currentPositionOfOp < position || (currentPositionOfOp == position && op.length() < shortOperator.length())))
              ) {
                op = shortOperator;
                position = currentPositionOfOp;
              }
            }
          }

          if (op != null) {
            String[] keyVal = new String[]{keyValuePair.substring(0, position).replaceAll(operatorComma, ","),
                keyValuePair.substring(position + op.length())
            };
            /** Cut from API-Gateway appended "=" */
            if ((">".equals(op) || "<".equals(op)) && keyVal[1].endsWith("=")) {
              keyVal[1] = keyVal[1].substring(0, keyVal[1].length() - 1);
            }

            propertyQuery.setKey(spaceProperties ? keyVal[0] : keyToPath(keyVal[0]));
            propertyQuery.setOperation(operators.get(op));
            String[] rawValues = keyVal[1].split(operatorComma);

            ArrayList<Object> values = new ArrayList<>();
            for (String rawValue : rawValues) {
              values.add(parseValue(rawValue));
            }
            propertyQuery.setValues(values);
            pql.add(propertyQuery);
          }
        });

    PropertiesQuery pq = new PropertiesQuery();
    pq.add(pql);

    if (pq.stream().flatMap(List::stream).mapToLong(l -> l.getValues().size()).sum() == 0) {
      return null;
    }
    return pq;
  }

  public @NotNull Map<@NotNull String, @Nullable Object> getAdditionalParams(@NotNull AdditionalParamsType additionalParamsType)
      throws XyzErrorException {
    Map<String, Object> clusteringParams = context.get(type);

    if (clusteringParams == null) {
      clusteringParams = parseAdditionalParams(context.request().query(), type);
      context.put(type, clusteringParams);
    }
    return clusteringParams;
  }

  static Map<String, Object> parseAdditionalParams(String query, String type) throws Exception {
    if (query == null || query.length() == 0) {
      return null;
    }

    final String paramPrefix = type + ".";

    Map<String, Object> cp = new HashMap<>();
    Stream.of(query.split("&"))
        .filter(k -> k.startsWith(paramPrefix))
        .forEach(keyValuePair -> {
          try {
            keyValuePair = URLDecoder.decode(keyValuePair, "utf-8");
          } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
          }

          if (keyValuePair.contains("=")) {
            // If the original parameter expression doesn't contain the equal sign API GW appends it at the end
            String[] keyVal = keyValuePair.split("=");
            if (keyVal.length < 2) {
              return;
            }
            String key = keyVal[0].substring(paramPrefix.length());
            Object value = parseValue(keyVal[1]);
            try {
              validateAdditionalParams(type, key, value);
            } catch (Exception e) {
              throw new RuntimeException(e.getMessage());
            }
            cp.put(keyVal[0].substring(paramPrefix.length()), parseValue(keyVal[1]));
          }
        });

    return cp;
  }

  private static void validateAdditionalParams(String type, String key, Object value) throws Exception {
    if (type.equals(CLUSTERING)) {
      String invalidKeyMessage = "Invalid clustering. " + key + " value. ";
      switch (key) {
        case CLUSTERING_PARAM_RESOLUTION_ABSOLUTE:
        case CLUSTERING_PARAM_RESOLUTION_RELATIVE:
        case CLUSTERING_PARAM_RESOLUTION:
          if (!(value instanceof Long)) {
            throw new Exception(invalidKeyMessage + "Expect Integer.");
          }

          if (CLUSTERING_PARAM_RESOLUTION_RELATIVE.equals(key) && ((long) value < -2 || (long) value > 4)) {
            throw new Exception(invalidKeyMessage + "Expect Integer hexbin:[-2,2], quadbin:[0-4].");
          }

          if (!CLUSTERING_PARAM_RESOLUTION_RELATIVE.equals(key) && ((long) value < 0 || (long) value > 18)) {
            throw new Exception(invalidKeyMessage + "Expect Integer hexbin:[0,13], quadbin:[0,18].");
          }
          break;

        case CLUSTERING_PARAM_PROPERTY:
          if (!(value instanceof String)) {
            throw new Exception(invalidKeyMessage + "Expect String.");
          }
          break;

        case CLUSTERING_PARAM_POINTMODE:
        case CLUSTERING_PARAM_SINGLECOORD:
        case CLUSTERING_PARAM_NOBUFFER:
          if (!(value instanceof Boolean)) {
            throw new Exception(invalidKeyMessage + "Expect true or false.");
          }
          break;

        case CLUSTERING_PARAM_COUNTMODE:
          if (!(value instanceof String)) {
            throw new Exception(invalidKeyMessage + "Expect one of [real,estimated,mixed].");
          }
          break;

        case CLUSTERING_PARAM_SAMPLING:
          if (!(value instanceof String)) {
            throw new Exception(invalidKeyMessage + "Expect one of [low,lowmed,med,medhigh,high].");
          }
          break;

        default:
          throw new Exception("Invalid Clustering Parameter! Expect one of ["
              + CLUSTERING_PARAM_RESOLUTION + "," + CLUSTERING_PARAM_RESOLUTION_RELATIVE + "," + CLUSTERING_PARAM_RESOLUTION_ABSOLUTE + ","
              + CLUSTERING_PARAM_PROPERTY + "," + CLUSTERING_PARAM_POINTMODE + "," + CLUSTERING_PARAM_COUNTMODE + ","
              + CLUSTERING_PARAM_SINGLECOORD + ","
              + CLUSTERING_PARAM_NOBUFFER + "," + CLUSTERING_PARAM_SAMPLING + "].");
      }
    } else if (type.equals(TWEAKS)) {
      switch (key) {
        case TWEAKS_PARAM_STRENGTH:
          if (value instanceof String) {
            String keyS = ((String) value).toLowerCase();
            switch (keyS) {
              case "low":
              case "lowmed":
              case "med":
              case "medhigh":
              case "high":
                break;
              default:
                throw new Exception("Invalid tweaks.strength value. Expect [low,lowmed,med,medhigh,high]");
            }
          } else if (value instanceof Long) {
            if ((long) value < 1 || (long) value > 100) {
              throw new Exception("Invalid tweaks.strength value. Expect Integer [1,100].");
            }
          } else {
            throw new Exception("Invalid tweaks.strength value. Expect String or Integer.");
          }

          break;

        case TWEAKS_PARAM_DEFAULT_SELECTION:
          if (!(value instanceof Boolean)) {
            throw new Exception("Invalid tweaks.defaultselection value. Expect true or false.");
          }
          break;

        case TWEAKS_PARAM_ALGORITHM:
          break;

        case TWEAKS_PARAM_SAMPLINGTHRESHOLD: // testing, parameter evaluation
          if (!(value instanceof Long) || ((long) value < 10) || ((long) value > 100)) {
            throw new Exception("Invalid tweaks. " + key + ". Expect Integer [10,100].");
          }
          break;

        default:
          throw new Exception("Invalid Tweaks Parameter! Expect one of [" + TWEAKS_PARAM_STRENGTH + ","
              + TWEAKS_PARAM_ALGORITHM + ","
              + TWEAKS_PARAM_DEFAULT_SELECTION + ","
              + TWEAKS_PARAM_SAMPLINGTHRESHOLD + "]");
      }

    }
  }


}
