package com.here.xyz.hub.params;

import static com.here.xyz.events.QueryParameterType.BOOLEAN;
import static com.here.xyz.events.QueryParameterType.DOUBLE;
import static com.here.xyz.events.QueryParameterType.LONG;
import static com.here.xyz.events.QueryParameterType.STRING;

import com.here.xyz.events.QueryOperation;
import com.here.xyz.events.PropertyQueryOr;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQueryAnd;
import com.here.xyz.events.QueryDelimiter;
import com.here.xyz.events.QueryParameter;
import com.here.xyz.events.QueryParameterType;
import com.here.xyz.events.QueryParameterList;
import com.here.xyz.events.TagList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.events.clustering.Clustering;
import com.here.xyz.events.tweaks.Tweaks;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.util.geo.GeoTools;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.util.ValueList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Special implementation for query parameters.
 */
public class XyzHubQueryParameters extends QueryParameterList {

  static final String ACCESS_TOKEN = "access_token";
  static final String ID = "id";
  static final String ADD_TAGS = "addTags";
  static final String REMOVE_TAGS = "removeTags";
  static final String TAGS = "tags";
  static final String QUERY = "query";
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
  static final String CLUSTERING_DOT = "clustering.";
  static final String TWEAKS = "tweaks";
  static final String TWEAKS_DOT = "tweaks.";
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

  /**
   * Creates a new query parameters map.
   *
   * @param queryString the query string without the question mark, for example {@code "foo=x&bar=y"}.
   * @throws ParameterError If parsing the query string failed.
   */
  public XyzHubQueryParameters(@Nullable String queryString) throws ParameterError {
    super(queryString, new XyzHubQueryDecoder());
  }

  /**
   * Returns the first value of the first parameter with the given key.
   *
   * @param key         The key to search for.
   * @param alternative The alternative to return, when no such parameter given.
   * @return The first value of the first parameter, if being a string; the alternative otherwise.
   * @throws ParameterError If any error occurred.
   */
  public @Nullable String getString(@NotNull String key, @Nullable String alternative) throws ParameterError {
    final QueryParameter queryParameter = get(key);
    if (queryParameter == null) {
      return alternative;
    }
    final ValueList values = queryParameter.values();
    if (values.size() > 1) {
      throw new ParameterError("Too many values for &" + key);
    }
    final String value = values.getString(0);
    if (value == null) {
      throw new ParameterError("&" + key + " expected to be a single string, but was: " + values.getOrNull(0));
    }
    return values.size() == 0 ? alternative : values.getString(0);
  }

  /**
   * Returns the first value of the first parameter with the given key.
   *
   * @param key         The key to search for.
   * @param alternative The alternative to return, when no such parameter given.
   * @return The first value of the first parameter, if being a boolean; the alternative otherwise.
   * @throws ParameterError If any error occurred.
   */
  public @Nullable Boolean getBoolean(@NotNull String key, @Nullable Boolean alternative) throws ParameterError {
    final QueryParameter queryParameter = get(key);
    if (queryParameter == null) {
      return alternative;
    }
    final ValueList values = queryParameter.values();
    if (values.size() > 1) {
      throw new ParameterError("Too many values for &" + key);
    }
    final Boolean value = values.getBoolean(0);
    return value == null ? alternative : value;
  }

  /**
   * Returns the first value of the first parameter with the given key.
   *
   * @param key      The key to search for.
   * @param minValue The minimum value allowed; if less, than an {@link ParameterError} is thrown.
   * @param maxValue The maximum value allowed; if less, than an {@link ParameterError} is thrown.
   * @return The first value of the first parameter, if being a double and within the given range; {@code null} otherwise.
   * @throws ParameterError If any error occurred.
   */
  public @Nullable Double getDouble(@NotNull String key, double minValue, double maxValue) throws ParameterError {
    final QueryParameter queryParameter = get(key);
    if (queryParameter == null) {
      return null;
    }
    final ValueList values = queryParameter.values();
    if (values.size() > 1) {
      throw new ParameterError("Too many values for &" + key);
    }
    final Double value = values.getDouble(0);
    if (value == null) {
      throw new ParameterError("&" + key + " expected to be a single double, but was: " + values.getOrNull(0));
    }
    if (value < minValue || value > maxValue) {
      throw new ParameterError("&" + key + " expected to be between [" + minValue + "," + maxValue + "], but was: " + value);
    }
    return value;
  }

  /**
   * Returns the first value of the first parameter with the given key as WGS'84 latitude.
   *
   * @param key The key to search for.
   * @return The first value of the first parameter, if being a double and within the latitude range; {@code null} otherwise.
   * @throws ParameterError If any error occurred.
   */
  public @Nullable Double getWgs84Latitude(@NotNull String key) throws ParameterError {
    return getDouble(key, -90d, +90d);
  }

  /**
   * Returns the first value of the first parameter with the given key as WGS'84 longitude.
   *
   * @param key The key to search for.
   * @return The first value of the first parameter, if being a double and within the longitude range; {@code null} otherwise.
   * @throws ParameterError If any error occurred.
   */
  public @Nullable Double getWgs84Longitude(@NotNull String key) throws ParameterError {
    return getDouble(key, -180d, +180d);
  }

  /**
   * Returns the first value of the first parameter with the given key.
   *
   * @param key      The key to search for.
   * @param minValue The minimum value allowed; if less, than an {@link ParameterError} is thrown.
   * @param maxValue The maximum value allowed; if less, than an {@link ParameterError} is thrown.
   * @return The first value of the first parameter, if being a long and within the given range; {@code null} otherwise.
   * @throws ParameterError If any error occurred.
   */
  public @Nullable Long getLong(@NotNull String key, long minValue, long maxValue) throws ParameterError {
    final QueryParameter queryParameter = get(key);
    if (queryParameter == null) {
      return null;
    }
    final ValueList values = queryParameter.values();
    if (values.size() > 1) {
      throw new ParameterError("Too many values for &" + key);
    }
    final Long value = values.getLong(0);
    if (value == null) {
      throw new ParameterError("&" + key + " expected to be a single double, but was: " + values.getOrNull(0));
    }
    if (value < minValue || value > maxValue) {
      throw new ParameterError("&" + key + " expected to be between [" + minValue + "," + maxValue + "], but was: " + value);
    }
    return value;
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
    if (__ids != null) {
      return __ids;
    }
    final QueryParameter id_param = join(ID);
    if (id_param == null) {
      return null;
    }
    return __ids = id_param.values().removeEmpty();
  }

  private List<@NotNull String> __ids;

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
    // We need to collect all "&tags" and merge them into tags-query.
    // Each &tags is combined via AND, within tags values are normally OR combined, except for the not URL encoded plus.
    // In that case we need to split the values into two separate AND, which effectively is the same as two "&tags" query parameters.
    QueryParameter tagParam = get(TAGS);
    TagsQuery and = new TagsQuery();
    while (tagParam != null) {
      final ValueList values = tagParam.values();
      final List<@NotNull QueryDelimiter> valueDelimiters = tagParam.valuesDelimiter();
      TagList query = null;
      for (int i = 0; i < values.size(); i++) {
        final String tag = values.getString(i);
        if (tag == null) {
          continue;
        }
        if (query == null) {
          query = new TagList();
        }
        query.addOr(tag);
        final QueryDelimiter delimiter = valueDelimiters.get(i);
        if (delimiter == QueryDelimiter.PLUS) {
          and.add(query);
          query = null;
        }
      }
      if (query != null) {
        and.add(query);
      }
      tagParam = tagParam.next();
    }
    return __tagsQuery = and;
  }

  private @Nullable List<@NotNull String> __selection;

  /**
   * Returns the property selection.
   *
   * @return The property selection; {@code null} if all properties selected.
   */
  public @Nullable List<@NotNull String> getSelection() {
    if (__selection != null) {
      return __selection;
    }
    final QueryParameter selectionParam = join(SELECTION);
    if (selectionParam == null) {
      return null;
    }
    return __selection = selectionParam.values().removeEmpty();
  }

  private @Nullable List<@NotNull String> __sort;

  /**
   * Returns the properties above which to order.
   *
   * @return The properties above which to order; {@code null} if no order selected.
   */
  public @Nullable List<@NotNull String> getSort() {
    if (__sort != null) {
      return __sort;
    }
    final QueryParameter selectionParam = join(SORT);
    if (selectionParam == null) {
      return null;
    }
    return __sort = selectionParam.values().removeEmpty();
  }

  private @Nullable XyzPart __part;

  public @Nullable XyzPart getPart() throws ParameterError {
    if (__part != null) {
      return __part;
    }
    final QueryParameter queryParameter = get(PART);
    if (queryParameter == null) {
      return null;
    }
    final ValueList values = queryParameter.values();
    if (values.size() != 2) {
      throw new ParameterError("Expected two two values: &part=(part,total)");
    }
    final Long part = values.getLong(0);
    final Long total = values.getLong(1);
    if (part == null || total == null || part < 1L || part > total || total > 1_000_000) {
      throw new ParameterError("Invalid argument &part=(" + values.get(0) + "," + values.get(1) + ")");
    }
    return __part = new XyzPart(part.intValue(), total.intValue());
  }

  /**
   * Returns the search radius in meter.
   *
   * @return The search radius in meter; {@code 0} if the parameter is not provided.
   * @throws ParameterError If the parameter is illegal.
   */
  public int getRadius() throws ParameterError {
    final QueryParameter queryParameter = get(RADIUS);
    if (queryParameter == null) {
      return 0;
    }
    final ValueList values = queryParameter.values();
    if (values.size() == 1) {
      final Long radius = values.getLong(0);
      if (radius == null || radius < 0L || radius > Integer.MAX_VALUE) {
        throw new ParameterError("Invalid parameter " + RADIUS + "=" + values.get(0) + ", expected positive int32");
      }
      return radius.intValue();
    }
    if (values.size() > 1) {
      throw new ParameterError("Invalid parameter " + RADIUS + ", expected positive int32");
    }
    return 0;
  }

  /**
   * The margin in pixel on the respective projected level around the tile; default is 0.
   *
   * @return the margin in pixel for a tile.
   */
  public int getMargin() throws ParameterError {
    final Long value = getLong(MARGIN, 0, 100);
    return (int) (value != null ? value : 0);
  }

  public @Nullable String getRefFeatureId() throws ParameterError {
    return getString(REF_FEATURE_ID, null);
  }

  public @Nullable String getRefSpaceId() throws ParameterError {
    return getString(REF_SPACE_ID, null);
  }

  public @Nullable String getH3Index() throws ParameterError {
    return getString(H3_INDEX, null);
  }

  public @Nullable String getOptimizationMode() throws ParameterError {
    return getString(OPTIM_MODE, null);
  }

  public @Nullable String getOptimizationVizSampling() throws ParameterError {
    return getString(OPTIM_VIZSAMPLING, null);
  }

  public boolean getForce2D() throws ParameterError {
    final Boolean value = getBoolean(FORCE_2D, null);
    return value != null ? value : false;
  }

  public boolean getClip() throws ParameterError {
    final Boolean value = getBoolean(CLIP, null);
    return value != null ? value : false;
  }

  public long getLimit() throws ParameterError {
    final Long limit = getLong(LIMIT, 0, 1_000_000);
    return limit != null ? limit : 10_000L;
  }

  private @Nullable BBox __bbox;

  public @Nullable BBox getBBox() throws ParameterError {
    if (__bbox != null) {
      return __bbox;
    }
    final Double west = getWgs84Longitude(WEST);
    if (west == null) {
      throw new ParameterError("Missing parameter " + WEST);
    }
    final Double east = getWgs84Longitude(EAST);
    if (east == null) {
      throw new ParameterError("Missing parameter " + EAST);
    }
    final Double north = getWgs84Longitude(NORTH);
    if (north == null) {
      throw new ParameterError("Missing parameter " + NORTH);
    }
    final Double south = getWgs84Longitude(SOUTH);
    if (south == null) {
      throw new ParameterError("Missing parameter " + SOUTH);
    }
    return __bbox = new BBox(west, south, east, north);
  }

  private @Nullable Point __point;

  /**
   * Returns the point encoded in the query parameters "&lat" and "&lon".
   *
   * @return the point parsed from query parameters; {@code null} if no point given.
   * @throws ParameterError If the parameters are illegal, for example only "&lat" provided.
   */
  public @Nullable Point getPoint() throws ParameterError {
    if (__point != null) {
      return __point;
    }
    final Double lat = getWgs84Latitude(LAT);
    final Double lon = getWgs84Longitude(LON);
    if (lat == null && lon == null) {
      return null;
    }
    if (lat == null) {
      throw new ParameterError("Missing &" + LAT + " parameter");
    }
    if (lon == null) {
      throw new ParameterError("Missing &" + LON + " parameter");
    }
    return __point = new Point().withCoordinates(new PointCoordinates(lon, lat));
  }

  private @Nullable PropertyQueryOr __query;

  /**
   * Returns the parsed properties query parameter.
   *
   * <p>Note: We fixed this so that we rewrite the old {@code &p.name=gt=5} into {@code &query:p*name=gt=5}. Still this design does not
   * allow the full potential of the query, so the outermost OR query is always useless.
   *
   * @return the parsed query parameter for space; {@code null} if no query parameter given.
   */
  public @Nullable PropertyQueryOr getPropertiesQuery() throws ParameterError {
    if (__query != null) {
      return __query;
    }
    QueryParameter queryParam = get(QUERY);
    if (queryParam == null) {
      return null;
    }
    final PropertyQueryOr outerQuery = new PropertyQueryOr();
    final PropertyQueryAnd and = new PropertyQueryAnd();
    outerQuery.add(and);

    while (queryParam != null) {
      final ValueList values = queryParam.values();
      final List<@NotNull QueryDelimiter> valuesDelimiter = queryParam.valuesDelimiter();
      final ValueList arguments = queryParam.arguments();
      final List<@NotNull QueryDelimiter> argumentsDelimiter = queryParam.argumentsDelimiter();

      final int LAST = values.size() - 1;
      if (LAST < 0) {
        throw new ParameterError("No value for query parameter: " + queryParam);
      }

      final QueryOperation op;
      if (arguments.size() >= 1) {
        final String opName = arguments.getString(0);
        if (opName != null) {
          op = QueryOperation.getByName(opName);
          if (op == null) {
            throw new ParameterError("Unknown operation '" + opName + "' for query parameter: " + queryParam);
          }
        } else {
          op = QueryOperation.EQUALS;
        }
      } else {
        op = QueryOperation.EQUALS;
      }
      PropertyQuery query = null;
      for (int i = 0; i <= LAST; i++) {
        final Object value = values.get(i);
        final QueryDelimiter delimiter = valuesDelimiter.get(i);
        if (query == null) {
          query = new PropertyQuery(queryParam.key(), op);
          and.add(query);
        }
        query.getValues().add(value);
        if (delimiter == QueryDelimiter.PLUS) {
          // A plus ends the current query, because the property query OR combines the values.
          query = null;
        }
      }
      queryParam = queryParam.next();
    }
    return outerQuery;
  }

  /**
   * Checks the query string for an EPSG code, when found, it passes and returns it. If not found, it will return the provided default
   * value.
   *
   * @return The EPSG selected.
   */
  public int getVerifiedEpsg() throws ParameterError {
    // See: https://en.wikipedia.org/wiki/EPSG_Geodetic_Parameter_Dataset
    final Long epsg = getLong(EPSG, 1024, 32767);
    if (epsg == null) {
      // WGS'84
      return 3785;
    }

    try {
      if (GeoTools.mathTransform(GeoTools.WGS84_EPSG, "EPSG:" + epsg) != null) {
        return epsg.intValue();
      }
    } catch (Exception ignore) {
    }
    throw new ParameterError(
        "Failed to transform coordinates from " + GeoTools.WGS84_EPSG + " to EPSG:" + epsg + ", unsupported Geodetic Parameter Dataset");
  }


  static final String CLUSTERING_PARAM_RESOLUTION = "resolution";
  static final String CLUSTERING_PARAM_RESOLUTION_RELATIVE = "relativeResolution";
  static final String CLUSTERING_PARAM_RESOLUTION_ABSOLUTE = "absoluteResolution";
  static final String CLUSTERING_PARAM_NOBUFFER = "noBuffer";
  static final String CLUSTERING_PARAM_PROPERTY = "property";
  static final String CLUSTERING_PARAM_POINTMODE = "pointmode";
  static final String CLUSTERING_PARAM_SINGLECOORD = "singlecoord";
  static final String CLUSTERING_PARAM_COUNTMODE = "countmode";
  static final String CLUSTERING_PARAM_SAMPLING = "sampling";

  /**
   * Create the clustering parameters from the given query parameters.
   *
   * @return The parsed clustering parameters.
   * @throws ParameterError if obligate parameters are missing or have illegal values.
   */
  public @NotNull Clustering getClustering() throws ParameterError {
    // TODO: We already should rewrite "&clustering.resolution=" into "&clustering:resolution=", which allows to grab all values from one
    //       key via normal parameters handling.
    QueryParameter clustering = get(CLUSTERING);
    while (clustering != null) {
      if (!clustering.hasArguments()) {
        // clustering = hexbin|quadbin
      } else {
        // clustering:parametername=value
        // Note: The operation theoretically can be as well greater-than or alike, e.g. "&clustering:age>100"!
      }
      clustering = clustering.next();
    }
    return null;
//    if (type.equals(CLUSTERING)) {
//      String invalidKeyMessage = "Invalid clustering. " + key + " value. ";
//      switch (key) {
//        case CLUSTERING_PARAM_RESOLUTION_ABSOLUTE:
//        case CLUSTERING_PARAM_RESOLUTION_RELATIVE:
//        case CLUSTERING_PARAM_RESOLUTION:
//          if (!(value instanceof Long)) {
//            throw new Exception(invalidKeyMessage + "Expect Integer.");
//          }
//
//          if (CLUSTERING_PARAM_RESOLUTION_RELATIVE.equals(key) && ((long) value < -2 || (long) value > 4)) {
//            throw new Exception(invalidKeyMessage + "Expect Integer hexbin:[-2,2], quadbin:[0-4].");
//          }
//
//          if (!CLUSTERING_PARAM_RESOLUTION_RELATIVE.equals(key) && ((long) value < 0 || (long) value > 18)) {
//            throw new Exception(invalidKeyMessage + "Expect Integer hexbin:[0,13], quadbin:[0,18].");
//          }
//          break;
//
//        case CLUSTERING_PARAM_PROPERTY:
//          if (!(value instanceof String)) {
//            throw new Exception(invalidKeyMessage + "Expect String.");
//          }
//          break;
//
//        case CLUSTERING_PARAM_POINTMODE:
//        case CLUSTERING_PARAM_SINGLECOORD:
//        case CLUSTERING_PARAM_NOBUFFER:
//          if (!(value instanceof Boolean)) {
//            throw new Exception(invalidKeyMessage + "Expect true or false.");
//          }
//          break;
//
//        case CLUSTERING_PARAM_COUNTMODE:
//          if (!(value instanceof String)) {
//            throw new Exception(invalidKeyMessage + "Expect one of [real,estimated,mixed].");
//          }
//          break;
//
//        case CLUSTERING_PARAM_SAMPLING:
//          if (!(value instanceof String)) {
//            throw new Exception(invalidKeyMessage + "Expect one of [low,lowmed,med,medhigh,high].");
//          }
//          break;
//
//        default:
//          throw new Exception("Invalid Clustering Parameter! Expect one of ["
//              + CLUSTERING_PARAM_RESOLUTION + "," + CLUSTERING_PARAM_RESOLUTION_RELATIVE + "," + CLUSTERING_PARAM_RESOLUTION_ABSOLUTE + ","
//              + CLUSTERING_PARAM_PROPERTY + "," + CLUSTERING_PARAM_POINTMODE + "," + CLUSTERING_PARAM_COUNTMODE + ","
//              + CLUSTERING_PARAM_SINGLECOORD + ","
//              + CLUSTERING_PARAM_NOBUFFER + "," + CLUSTERING_PARAM_SAMPLING + "].");
//      }
  }

  /**
   * Create the tweaks parameters from the given query parameters.
   *
   * @return The parsed tweaks parameters.
   * @throws ParameterError if obligate parameters are missing or have illegal values.
   */
  public @NotNull Tweaks getTweaks() throws ParameterError {
    // TODO: We already should rewrite "&tweak.resolution=" into "&tweak:resolution=", which allows to grab all values from one
    //       key via normal parameters handling.
    QueryParameter tweaks = get(TWEAKS);
    while (tweaks != null) {
      if (!tweaks.hasArguments()) {
        // tweaks = sampling|simplification|ensure
      } else {
        // tweaks:parametername=value
        // Note: The operation theoretically can be as well greater-than or alike, e.g. "&tweak:age>100"!
      }
      tweaks = tweaks.next();
    }
    return null;
//    if (type.equals(TWEAKS)) {
//      switch (key) {
//        case TWEAKS_PARAM_STRENGTH:
//          if (value instanceof String) {
//            String keyS = ((String) value).toLowerCase();
//            switch (keyS) {
//              case "low":
//              case "lowmed":
//              case "med":
//              case "medhigh":
//              case "high":
//                break;
//              default:
//                throw new Exception("Invalid tweaks.strength value. Expect [low,lowmed,med,medhigh,high]");
//            }
//          } else if (value instanceof Long) {
//            if ((long) value < 1 || (long) value > 100) {
//              throw new Exception("Invalid tweaks.strength value. Expect Integer [1,100].");
//            }
//          } else {
//            throw new Exception("Invalid tweaks.strength value. Expect String or Integer.");
//          }
//
//          break;
//
//        case TWEAKS_PARAM_DEFAULT_SELECTION:
//          if (!(value instanceof Boolean)) {
//            throw new Exception("Invalid tweaks.defaultselection value. Expect true or false.");
//          }
//          break;
//
//        case TWEAKS_PARAM_ALGORITHM:
//          break;
//
//        case TWEAKS_PARAM_SAMPLINGTHRESHOLD: // testing, parameter evaluation
//          if (!(value instanceof Long) || ((long) value < 10) || ((long) value > 100)) {
//            throw new Exception("Invalid tweaks. " + key + ". Expect Integer [10,100].");
//          }
//          break;
//
//        default:
//          throw new Exception("Invalid Tweaks Parameter! Expect one of [" + TWEAKS_PARAM_STRENGTH + ","
//              + TWEAKS_PARAM_ALGORITHM + ","
//              + TWEAKS_PARAM_DEFAULT_SELECTION + ","
//              + TWEAKS_PARAM_SAMPLINGTHRESHOLD + "]");
//      }
//
//    }  }
  }
}
