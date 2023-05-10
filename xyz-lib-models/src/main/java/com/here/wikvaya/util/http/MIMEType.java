package com.here.wikvaya.util.http;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MIMEType {

  /**
   * The global static hash map that contains all known MIME types in lower cased notation as key and in the correct cased version as value.
   * This can be used for example in switch-case.
   */
  public static final ConcurrentHashMap<String, String> LOWERCASE =
      new ConcurrentHashMap<String, String>(
          2048, 0.5f, Runtime.getRuntime().availableProcessors() * 2);

  /**
   * Can be invoked to register a mime type in the {@link #LOWERCASE lower case} hash map.
   *
   * @param mimeType the mime type to register.
   * @return the mime type as given.
   */
  public static String addLowerCaseMimeType(final String mimeType) {
    if (mimeType == null) {
      return null;
    }
    LOWERCASE.putIfAbsent(mimeType.toLowerCase(), mimeType);
    return mimeType;
  }

  /**
   * Can be invoked to register multiple mime types in the {@link #LOWERCASE lower case} hash map.
   *
   * @param mimeType the mime types to register.
   */
  protected static void addLowerCaseMimeTypes(final String... mimeType) {
    if (mimeType != null) {
      for (final String s : mimeType) {
        if (s != null) {
          LOWERCASE.put(s.toLowerCase(), s);
        }
      }
    }
  }

  /**
   * Fixes the casing of the mime type.
   *
   * @param mimeType the mime type without parameters (so only up to the first semicolon).
   * @return the same mime type in the desired casing, if available; otherwise the unchanged given mime type.
   */
  public static String getMimeType(final String mimeType) {
    if (mimeType == null) {
      return null;
    }
    String fixed = LOWERCASE.get(mimeType);
    if (fixed != null) {
      return fixed;
    }
    fixed = LOWERCASE.get(mimeType.toLowerCase());
    return fixed == null ? mimeType : fixed;
  }

  /**
   * The global static hash map that defines which MIME types are JSON. Note that the key must be lower cased and the value must be always
   * {@link Boolean#TRUE}. To check if a certain MIME type is JSON, simply check if this hash map contains the corresponding MIME type as
   * key. Take care that the MIME type is lower cased!
   */
  protected static final ConcurrentHashMap<String, Boolean> JSON = new ConcurrentHashMap<>();

  /**
   * The global static hash map that defines which MIME types are TEXT. Note that the key must be lower cased and the value must be always
   * {@link Boolean#TRUE}. To check if a certain MIME type is TEXT, simply check if this hash map contains the corresponding MIME type as
   * key. Take care that the MIME type is lower cased!
   */
  protected static final ConcurrentHashMap<String, Boolean> TEXT = new ConcurrentHashMap<>();

  /**
   * The global static hash map that defines which MIME types are XML. Note that the key must be lower cased and the value must be always
   * {@link Boolean#TRUE}. To check if a certain MIME type is XML, simply check if this hash map contains the corresponding MIME type as
   * key. Take care that the MIME type is lower cased!
   */
  protected static final ConcurrentHashMap<String, Boolean> XML = new ConcurrentHashMap<>();

  /**
   * Tests whether the given parameterless mime type is a JSON.
   *
   * @param mimeType the mime-type to test.
   * @return true if this mime type is JSON.
   */
  public static boolean isJSON(String mimeType) {
    if (mimeType == null) {
      return false;
    }
    if (JSON.containsKey(mimeType)) {
      return true;
    }
    if (JSON.containsKey(mimeType.toLowerCase())) {
      return true;
    }
    return false;
  }

  /**
   * Tests whether the given parameterless mime type is a TEXT. Note that JSON is always TEXT, but not all TEXT is as well JSON.
   *
   * @param mimeType the mime-type to test.
   * @return true if this mime type is TEXT.
   */
  public static boolean isTEXT(String mimeType) {
    if (mimeType == null) {
      return false;
    }
    if (TEXT.containsKey(mimeType)) {
      return true;
    }
    if (TEXT.containsKey(mimeType.toLowerCase())) {
      return true;
    }
    return false;
  }

  /**
   * Tests whether the given parameterless mime type is a XML.
   *
   * @param mimeType the mime-type to test.
   * @return true if this mime type is XML.
   */
  public static boolean isXML(String mimeType) {
    if (mimeType == null) {
      return false;
    }
    if (XML.containsKey(mimeType)) {
      return true;
    }
    if (XML.containsKey(mimeType.toLowerCase())) {
      return true;
    }
    return false;
  }

  /**
   * Marks the provided mime type as JSON and returns it.
   *
   * @param mimeType the mime type to mark as being JSON.
   * @return the mime type as provided.
   */
  protected static String json(final String mimeType) {
    if (mimeType == null) {
      return null;
    }
    final String mimeTypeLC = mimeType.toLowerCase();
    TEXT.put(mimeType, Boolean.TRUE);
    TEXT.put(mimeTypeLC, Boolean.TRUE);
    JSON.put(mimeType, Boolean.TRUE);
    JSON.put(mimeTypeLC, Boolean.TRUE);
    LOWERCASE.put(mimeTypeLC, mimeType);
    return mimeType;
  }

  /**
   * Marks the provided mime type as XML and returns it.
   *
   * @param mimeType the mime type to mark as being XML.
   * @return the mime type as provided.
   */
  protected static String xml(final String mimeType) {
    if (mimeType == null) {
      return null;
    }
    final String mimeTypeLC = mimeType.toLowerCase();
    TEXT.put(mimeType, Boolean.TRUE);
    TEXT.put(mimeTypeLC, Boolean.TRUE);
    XML.put(mimeType, Boolean.TRUE);
    XML.put(mimeTypeLC, Boolean.TRUE);
    LOWERCASE.put(mimeTypeLC, mimeType);
    return mimeType;
  }

  /**
   * Marks the provided mime type as TEXT and returns it.
   *
   * @param mimeType the mime type to mark as being TEXT.
   * @return the mime type as provided.
   */
  protected static String text(final String mimeType) {
    if (mimeType == null) {
      return null;
    }
    final String mimeTypeLC = mimeType.toLowerCase();
    TEXT.put(mimeType, Boolean.TRUE);
    TEXT.put(mimeTypeLC, Boolean.TRUE);
    LOWERCASE.put(mimeTypeLC, mimeType);
    return mimeType;
  }

  // --- Custom MIME types used by Wikvaya
  public static final String APPLICATION_VND_HERE_WIKVAYA_VDATA_JSON =
      "application/vnd.here.wikvaya.vdata+json";
  public static final String APPLICATION_VND_HERE_WIKVAYA_REVERTED_AND_REMOVED_IDS =
      "application/vnd.here.wikvaya.revertedAndRemovedIds+json";
  public static final String APPLICATION_VND_HERE_WIKVAYA_ACCESS_CONTROL_MATRIX_JSON =
      "application/vnd.here.wikvaya.AccessControlMatrix+json";
  public static final String APPLICATION_VND_HERE_WIKVAYA_ACCESS_MATRIX_JSON =
      "application/vnd.here.wikvaya.AccessMatrix+json";

  static {
    json(APPLICATION_VND_HERE_WIKVAYA_VDATA_JSON);
    json(APPLICATION_VND_HERE_WIKVAYA_REVERTED_AND_REMOVED_IDS);
    json(APPLICATION_VND_HERE_WIKVAYA_ACCESS_CONTROL_MATRIX_JSON);
    json(APPLICATION_VND_HERE_WIKVAYA_ACCESS_MATRIX_JSON);
  }

  // --- Custom MIME types used by Map Creator
  public static final String APPLICATION_VND_HERE_MAPCREATOR_NAVLINK_V7_4 =
      "application/vnd.here.mapcreator.navlink.v7.4+json";
  public static final String APPLICATION_VND_HERE_MAPCREATOR_POI_V7_4 =
      "application/vnd.here.mapcreator.poi.v7.4+json";
  public static final String APPLICATION_VND_HERE_MAPCREATOR_PA_V7_4 =
      "application/vnd.here.mapcreator.pa.v7.4+json";
  public static final String APPLICATION_VND_HERE_MAPCREATOR_GEOINFO =
      "application/vnd.here.mapcreator.geoInfo+json";
  public static final String APPLICATION_VND_HERE_MAPCREATOR_GEOINFO_LIST =
      "application/vnd.here.mapcreator.geoInfoList+json";

  static {
    json(APPLICATION_VND_HERE_MAPCREATOR_NAVLINK_V7_4);
    json(APPLICATION_VND_HERE_MAPCREATOR_POI_V7_4);
    json(APPLICATION_VND_HERE_MAPCREATOR_PA_V7_4);
    json(APPLICATION_VND_HERE_MAPCREATOR_GEOINFO);
    json(APPLICATION_VND_HERE_MAPCREATOR_GEOINFO_LIST);
  }

  // --- Custom MIME types used by the MapHub service
  public static final String APPLICATION_VND_HERE_LAYER_JSON = "application/vnd.here.layer+json";
  public static final String APPLICATION_VND_HERE_LAYER_LIST_JSON =
      "application/vnd.here.layerList+json";
  public static final String APPLICATION_VND_HERE_LAYER_PAGE_JSON =
      "application/vnd.here.layerPage+json";
  public static final String APPLICATION_VND_HERE_LAYER_OBJECT_PAGE_JSON =
      "application/vnd.here.layerObjectPage+json";
  public static final String APPLICATION_VND_HERE_LAYER_OBJECT_JSON =
      "application/vnd.here.layerObject+json";
  public static final String APPLICATION_VND_HERE_LAYER_OBJECT_LIST_JSON =
      "application/vnd.here.layerObjectList+json";
  public static final String APPLICATION_VND_HERE_FILTER_JSON = "application/vnd.here.filter+json";
  public static final String APPLICATION_VND_HERE_FILTER_LIST_JSON =
      "application/vnd.here.filterList+json";
  public static final String APPLICATION_VND_HERE_LINE_JSON = "application/vnd.here.line+json";
  public static final String APPLICATION_VND_HERE_ID_LIST_JSON = "application/vnd.here.idList+json";
  public static final String APPLICATION_VND_HERE_ID_JSON = "application/vnd.here.id+json";
  public static final String APPLICATION_VND_HERE_REVISION_JSON =
      "application/vnd.here.revision+json";
  public static final String APPLICATION_VND_HERE_REVISION_PAGE_JSON =
      "application/vnd.here.revisionPage+json";
  public static final String APPLICATION_VND_HERE_CHANGE_KERNEL_JSON =
      "application/vnd.here.changeKernel+json";
  public static final String APPLICATION_VND_HERE_CHANGE_KERNEL_PAGE_JSON =
      "application/vnd.here.changeKernelPage+json";
  public static final String APPLICATION_VND_HERE_STATIC_ENTRY_LIST_JSON =
      "application/vnd.here.statisticEntryList+json";
  public static final String APPLICATION_VND_HERE_USER_JSON = "application/vnd.here.user+json";
  public static final String APPLICATION_VND_HERE_USER_ACL_JSON =
      "application/vnd.here.userACL+json";
  public static final String APPLICATION_VND_HERE_USER_ROLE_LIST_JSON =
      "application/vnd.here.userRoleList+json";
  public static final String APPLICATION_VND_HERE_ENVIRONMENT_JSON =
      "application/vnd.here.environment+json";
  public static final String APPLICATION_VND_HERE_VIEW_JSON = "application/vnd.here.view+json";
  public static final String APPLICATION_VND_HERE_VIEW_LIST_JSON =
      "application/vnd.here.viewList+json";
  public static final String APPLICATION_VND_HERE_MOM_OBJECT_PAGE_JSON =
      "application/vnd.here.momObjectPage+json";
  public static final String APPLICATION_VND_HERE_MOM_OBJECT_LIST_JSON =
      "application/geo+json;format=mom";

  static {
    json(APPLICATION_VND_HERE_LAYER_JSON);
    json(APPLICATION_VND_HERE_LAYER_LIST_JSON);
    json(APPLICATION_VND_HERE_LAYER_PAGE_JSON);
    json(APPLICATION_VND_HERE_LAYER_OBJECT_PAGE_JSON);
    json(APPLICATION_VND_HERE_LAYER_OBJECT_JSON);
    json(APPLICATION_VND_HERE_LAYER_OBJECT_LIST_JSON);
    json(APPLICATION_VND_HERE_FILTER_JSON);
    json(APPLICATION_VND_HERE_FILTER_LIST_JSON);
    json(APPLICATION_VND_HERE_LINE_JSON);
    json(APPLICATION_VND_HERE_ID_LIST_JSON);
    json(APPLICATION_VND_HERE_ID_JSON);
    json(APPLICATION_VND_HERE_REVISION_JSON);
    json(APPLICATION_VND_HERE_REVISION_PAGE_JSON);
    json(APPLICATION_VND_HERE_CHANGE_KERNEL_JSON);
    json(APPLICATION_VND_HERE_CHANGE_KERNEL_PAGE_JSON);
    json(APPLICATION_VND_HERE_STATIC_ENTRY_LIST_JSON);
    json(APPLICATION_VND_HERE_USER_JSON);
    json(APPLICATION_VND_HERE_USER_ACL_JSON);
    json(APPLICATION_VND_HERE_USER_ROLE_LIST_JSON);
    json(APPLICATION_VND_HERE_ENVIRONMENT_JSON);
    json(APPLICATION_VND_HERE_VIEW_JSON);
    json(APPLICATION_VND_HERE_VIEW_LIST_JSON);
    json(APPLICATION_VND_HERE_MOM_OBJECT_PAGE_JSON);
    json(APPLICATION_VND_HERE_MOM_OBJECT_LIST_JSON);
  }

  public static final String APPLICATION_VND_NAVTEQ_LAYER_JSON =
      "application/vnd.navteq.layer+json";
  public static final String APPLICATION_VND_NAVTEQ_LAYER_LIST_JSON =
      "application/vnd.navteq.layerList+json";
  public static final String APPLICATION_VND_NAVTEQ_LAYER_PAGE_JSON =
      "application/vnd.navteq.layerPage+json";
  public static final String APPLICATION_VND_NAVTEQ_LAYER_OBJECT_PAGE_JSON =
      "application/vnd.navteq.layerObjectPage+json";
  public static final String APPLICATION_VND_NAVTEQ_LAYER_OBJECT_JSON =
      "application/vnd.navteq.layerObject+json";
  public static final String APPLICATION_VND_NAVTEQ_LAYER_OBJECT_LIST_JSON =
      "application/vnd.navteq.layerObjectList+json";
  public static final String APPLICATION_VND_NAVTEQ_FILTER_JSON =
      "application/vnd.navteq.filter+json";
  public static final String APPLICATION_VND_NAVTEQ_FILTER_LIST_JSON =
      "application/vnd.navteq.filterList+json";
  public static final String APPLICATION_VND_NAVTEQ_LINE_JSON = "application/vnd.navteq.line+json";
  public static final String APPLICATION_VND_NAVTEQ_ID_LIST_JSON =
      "application/vnd.navteq.idList+json";
  public static final String APPLICATION_VND_NAVTEQ_ID_JSON = "application/vnd.navteq.id+json";
  public static final String APPLICATION_VND_NAVTEQ_REVISION_JSON =
      "application/vnd.navteq.revision+json";
  public static final String APPLICATION_VND_NAVTEQ_REVISION_PAGE_JSON =
      "application/vnd.navteq.revisionPage+json";
  public static final String APPLICATION_VND_NAVTEQ_CHANGE_KERNEL_JSON =
      "application/vnd.navteq.changeKernel+json";
  public static final String APPLICATION_VND_NAVTEQ_CHANGE_KERNEL_PAGE_JSON =
      "application/vnd.navteq.changeKernelPage+json";
  public static final String APPLICATION_VND_NAVTEQ_STATIC_ENTRY_LIST_JSON =
      "application/vnd.navteq.statisticEntryList+json";
  public static final String APPLICATION_VND_NAVTEQ_USER_JSON = "application/vnd.navteq.user+json";
  public static final String APPLICATION_VND_NAVTEQ_USER_ACL_JSON =
      "application/vnd.navteq.userACL+json";
  public static final String APPLICATION_VND_NAVTEQ_USER_ROLE_LIST_JSON =
      "application/vnd.navteq.userRoleList+json";

  static {
    json(APPLICATION_VND_NAVTEQ_LAYER_JSON);
    json(APPLICATION_VND_NAVTEQ_LAYER_LIST_JSON);
    json(APPLICATION_VND_NAVTEQ_LAYER_PAGE_JSON);
    json(APPLICATION_VND_NAVTEQ_LAYER_OBJECT_PAGE_JSON);
    json(APPLICATION_VND_NAVTEQ_LAYER_OBJECT_JSON);
    json(APPLICATION_VND_NAVTEQ_LAYER_OBJECT_LIST_JSON);
    json(APPLICATION_VND_NAVTEQ_FILTER_JSON);
    json(APPLICATION_VND_NAVTEQ_FILTER_LIST_JSON);
    json(APPLICATION_VND_NAVTEQ_LINE_JSON);
    json(APPLICATION_VND_NAVTEQ_ID_LIST_JSON);
    json(APPLICATION_VND_NAVTEQ_ID_JSON);
    json(APPLICATION_VND_NAVTEQ_REVISION_JSON);
    json(APPLICATION_VND_NAVTEQ_REVISION_PAGE_JSON);
    json(APPLICATION_VND_NAVTEQ_CHANGE_KERNEL_JSON);
    json(APPLICATION_VND_NAVTEQ_CHANGE_KERNEL_PAGE_JSON);
    json(APPLICATION_VND_NAVTEQ_STATIC_ENTRY_LIST_JSON);
    json(APPLICATION_VND_NAVTEQ_USER_JSON);
    json(APPLICATION_VND_NAVTEQ_USER_ACL_JSON);
    json(APPLICATION_VND_NAVTEQ_USER_ROLE_LIST_JSON);
  }

  public static final String MULTIPART_FORM_DATA = "multipart/form-data";

  public static boolean isLayer(String mimeType) {
    return APPLICATION_VND_HERE_LAYER_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_LAYER_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isLayerList(String mimeType) {
    return APPLICATION_VND_HERE_LAYER_LIST_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_LAYER_LIST_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isLayerPage(String mimeType) {
    return APPLICATION_VND_HERE_LAYER_PAGE_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_LAYER_PAGE_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isLayerObjectPage(String mimeType) {
    return APPLICATION_VND_HERE_LAYER_OBJECT_PAGE_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_LAYER_OBJECT_PAGE_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isLayerObject(String mimeType) {
    return APPLICATION_VND_HERE_LAYER_OBJECT_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_LAYER_OBJECT_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isLayerObjectList(String mimeType) {
    return APPLICATION_VND_HERE_LAYER_OBJECT_LIST_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_LAYER_OBJECT_LIST_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isFilter(String mimeType) {
    return APPLICATION_VND_HERE_FILTER_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_FILTER_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isFilterList(String mimeType) {
    return APPLICATION_VND_HERE_FILTER_LIST_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_FILTER_LIST_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isLine(String mimeType) {
    return APPLICATION_VND_HERE_LINE_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_LINE_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isIdList(String mimeType) {
    return APPLICATION_VND_HERE_ID_LIST_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_ID_LIST_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isId(String mimeType) {
    return APPLICATION_VND_HERE_ID_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_ID_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isRevision(String mimeType) {
    return APPLICATION_VND_HERE_REVISION_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_REVISION_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isRevisionPage(String mimeType) {
    return APPLICATION_VND_HERE_REVISION_PAGE_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_REVISION_PAGE_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isChangeKernel(String mimeType) {
    return APPLICATION_VND_HERE_CHANGE_KERNEL_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_CHANGE_KERNEL_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isChangeKernelPage(String mimeType) {
    return APPLICATION_VND_HERE_CHANGE_KERNEL_PAGE_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_CHANGE_KERNEL_PAGE_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isStaticEntryList(String mimeType) {
    return APPLICATION_VND_HERE_STATIC_ENTRY_LIST_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_STATIC_ENTRY_LIST_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isUser(String mimeType) {
    return APPLICATION_VND_HERE_USER_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_USER_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isUserACL(String mimeType) {
    return APPLICATION_VND_HERE_USER_ACL_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_NAVTEQ_USER_ACL_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isUserRoleList(String mimeType) {
    return APPLICATION_VND_NAVTEQ_USER_ROLE_LIST_JSON.equalsIgnoreCase(mimeType)
        || APPLICATION_VND_HERE_USER_ROLE_LIST_JSON.equalsIgnoreCase(mimeType);
  }

  public static boolean isGeoJson(String mimeType) {
    return APPLICATION_GEO_JSON.equalsIgnoreCase(mimeType);
  }

  // ---
  public static final String APPLICATION_ANDREW_INSET = "application/andrew-inset";
  public static final String APPLICATION_APPLIXWARE = "application/applixware";
  public static final String APPLICATION_ATOM_XML = "application/atom+xml";
  public static final String APPLICATION_ATOMCAT_XML = "application/atomcat+xml";
  public static final String APPLICATION_ATOMSVC_XML = "application/atomsvc+xml";
  public static final String APPLICATION_CCXML_XML = "application/ccxml+xml";
  public static final String APPLICATION_CDMI_CAPABILITY = "application/cdmi-capability";
  public static final String APPLICATION_CDMI_CONTAINER = "application/cdmi-container";
  public static final String APPLICATION_CDMI_DOMAIN = "application/cdmi-domain";
  public static final String APPLICATION_CDMI_OBJECT = "application/cdmi-object";
  public static final String APPLICATION_CDMI_QUEUE = "application/cdmi-queue";
  public static final String APPLICATION_CU_SEEME = "application/cu-seeme";
  public static final String APPLICATION_DAVMOUNT_XML = "application/davmount+xml";
  public static final String APPLICATION_DOCBOOK_XML = "application/docbook+xml";
  public static final String APPLICATION_DSSC_DER = "application/dssc+der";
  public static final String APPLICATION_DSSC_XML = "application/dssc+xml";
  public static final String APPLICATION_ECMASCRIPT = "application/ecmascript";
  public static final String APPLICATION_EMMA_XML = "application/emma+xml";
  public static final String APPLICATION_EPUB_ZIP = "application/epub+zip";
  public static final String APPLICATION_EXI = "application/exi";
  public static final String APPLICATION_FONT_TDPFR = "application/font-tdpfr";
  public static final String APPLICATION_GML_XML = "application/gml+xml";
  public static final String APPLICATION_GPX_XML = "application/gpx+xml";
  public static final String APPLICATION_GEO_JSON = "application/geo+json";

  static {
    xml(APPLICATION_ATOM_XML);
    xml(APPLICATION_ATOMCAT_XML);
    xml(APPLICATION_ATOMSVC_XML);
    xml(APPLICATION_CCXML_XML);
    xml(APPLICATION_DAVMOUNT_XML);
    xml(APPLICATION_DOCBOOK_XML);
    xml(APPLICATION_DAVMOUNT_XML);
    xml(APPLICATION_DSSC_XML);
    xml(APPLICATION_EMMA_XML);
    xml(APPLICATION_GML_XML);
    xml(APPLICATION_GPX_XML);
    json(APPLICATION_GEO_JSON);
  }

  public static final String APPLICATION_VND_MAPBOX_VECTOR_TILE =
      "application/vnd.mapbox-vector-tile"; // Mapbox Vector Tile
  public static final String APPLICATION_GXF = "application/gxf";
  public static final String APPLICATION_GZIP = "application/gzip";
  public static final String APPLICATION_HYPERSTUDIO = "application/hyperstudio";
  public static final String APPLICATION_INKML_XML = "application/inkml+xml";
  public static final String APPLICATION_IPFIX = "application/ipfix";
  public static final String APPLICATION_JAVA_ARCHIVE = "application/java-archive";
  public static final String APPLICATION_JAVA_SERIALIZED_OBJECT =
      "application/java-serialized-object";
  public static final String APPLICATION_JAVA_VM = "application/java-vm";
  public static final String APPLICATION_JAVASCRIPT = "application/javascript";
  public static final String APPLICATION_JSON = "application/json";

  static {
    xml(APPLICATION_INKML_XML);
    text(APPLICATION_JAVASCRIPT);
    json(APPLICATION_JSON);
  }

  public static final String APPLICATION_JSONML_JSON = "application/jsonml+json";

  static {
    json(APPLICATION_JSONML_JSON);
  }

  public static final String APPLICATION_LOST_XML = xml("application/lost+xml");
  public static final String APPLICATION_MAC_BINHEX40 = "application/mac-binhex40";
  public static final String APPLICATION_MAC_COMPACTPRO = "application/mac-compactpro";
  public static final String APPLICATION_MADS_XML = xml("application/mads+xml");
  public static final String APPLICATION_MARC = "application/marc";
  public static final String APPLICATION_MARCXML_XML = xml("application/marcxml+xml");
  public static final String APPLICATION_MATHEMATICA = "application/mathematica";
  public static final String APPLICATION_MATHML_XML = xml("application/mathml+xml");
  public static final String APPLICATION_MBOX = "application/mbox";
  public static final String APPLICATION_MEDIASERVERCONTROL_XML =
      xml("application/mediaservercontrol+xml");
  public static final String APPLICATION_METALINK_XML = xml("application/metalink+xml");
  public static final String APPLICATION_METALINK4_XML = xml("application/metalink4+xml");
  public static final String APPLICATION_METS_XML = xml("application/mets+xml");
  public static final String APPLICATION_MODS_XML = xml("application/mods+xml");
  public static final String APPLICATION_MP21 = "application/mp21";
  public static final String APPLICATION_MP4 = "application/mp4";
  public static final String APPLICATION_MSWORD = "application/msword";
  public static final String APPLICATION_MXF = "application/mxf";
  public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
  public static final String APPLICATION_ODA = "application/oda";
  public static final String APPLICATION_OEBPS_PACKAGE_XML = "application/oebps-package+xml";
  public static final String APPLICATION_OGG = "application/ogg";
  public static final String APPLICATION_OMDOC_XML = xml("application/omdoc+xml");
  public static final String APPLICATION_ONENOTE = "application/onenote";
  public static final String APPLICATION_OXPS = "application/oxps";
  public static final String APPLICATION_PATCH_OPS_ERROR_XML =
      xml("application/patch-ops-error+xml");
  public static final String APPLICATION_PDF = "application/pdf";
  public static final String APPLICATION_PGP_ENCRYPTED = "application/pgp-encrypted";
  public static final String APPLICATION_PGP_SIGNATURE = "application/pgp-signature";
  public static final String APPLICATION_PICS_RULES = "application/pics-rules";
  public static final String APPLICATION_PKCS10 = "application/pkcs10";
  public static final String APPLICATION_PKCS7_MIME = "application/pkcs7-mime";
  public static final String APPLICATION_PKCS7_SIGNATURE = "application/pkcs7-signature";
  public static final String APPLICATION_PKCS8 = "application/pkcs8";
  public static final String APPLICATION_PKIX_ATTR_CERT = "application/pkix-attr-cert";
  public static final String APPLICATION_PKIX_CERT = "application/pkix-cert";
  public static final String APPLICATION_PKIX_CRL = "application/pkix-crl";
  public static final String APPLICATION_PKIX_PKIPATH = "application/pkix-pkipath";
  public static final String APPLICATION_PKIXCMP = "application/pkixcmp";
  public static final String APPLICATION_PLS_XML = xml("application/pls+xml");
  public static final String APPLICATION_POSTSCRIPT = "application/postscript";
  public static final String APPLICATION_PRS_CWW = "application/prs.cww";
  public static final String APPLICATION_PSKC_XML = xml("application/pskc+xml");
  public static final String APPLICATION_RDF_XML = xml("application/rdf+xml");
  public static final String APPLICATION_REGINFO_XML = xml("application/reginfo+xml");
  public static final String APPLICATION_RELAX_NG_COMPACT_SYNTAX =
      "application/relax-ng-compact-syntax";
  public static final String APPLICATION_RESOURCE_LISTS_XML = xml("application/resource-lists+xml");
  public static final String APPLICATION_RESOURCE_LISTS_DIFF_XML =
      xml("application/resource-lists-diff+xml");
  public static final String APPLICATION_RLS_SERVICES_XML = xml("application/rls-services+xml");
  public static final String APPLICATION_RPKI_GHOSTBUSTERS = "application/rpki-ghostbusters";
  public static final String APPLICATION_RPKI_MANIFEST = "application/rpki-manifest";
  public static final String APPLICATION_RPKI_ROA = "application/rpki-roa";
  public static final String APPLICATION_RSD_XML = xml("application/rsd+xml");
  public static final String APPLICATION_RSS_XML = xml("application/rss+xml");
  public static final String APPLICATION_RTF = "application/rtf";
  public static final String APPLICATION_SBML_XML = xml("application/sbml+xml");
  public static final String APPLICATION_SCVP_CV_REQUEST = "application/scvp-cv-request";
  public static final String APPLICATION_SCVP_CV_RESPONSE = "application/scvp-cv-response";
  public static final String APPLICATION_SCVP_VP_REQUEST = "application/scvp-vp-request";
  public static final String APPLICATION_SCVP_VP_RESPONSE = "application/scvp-vp-response";
  public static final String APPLICATION_SDP = "application/sdp";
  public static final String APPLICATION_SET_PAYMENT_INITIATION =
      "application/set-payment-initiation";
  public static final String APPLICATION_SET_REGISTRATION_INITIATION =
      "application/set-registration-initiation";
  public static final String APPLICATION_SHF_XML = xml("application/shf+xml");
  public static final String APPLICATION_SMIL_XML = xml("application/smil+xml");
  public static final String APPLICATION_SPARQL_QUERY = "application/sparql-query";
  public static final String APPLICATION_SPARQL_RESULTS_XML = xml("application/sparql-results+xml");
  public static final String APPLICATION_SRGS = "application/srgs";
  public static final String APPLICATION_SRGS_XML = xml("application/srgs+xml");
  public static final String APPLICATION_SRU_XML = xml("application/sru+xml");
  public static final String APPLICATION_SSDL_XML = xml("application/ssdl+xml");
  public static final String APPLICATION_SSML_XML = xml("application/ssml+xml");
  public static final String APPLICATION_TEI_XML = xml("application/tei+xml");
  public static final String APPLICATION_THRAUD_XML = xml("application/thraud+xml");
  public static final String APPLICATION_TIMESTAMPED_DATA = "application/timestamped-data";
  public static final String APPLICATION_TRACE_GPS = "application/trace+gps";
  public static final String APPLICATION_VND_3GPP_PIC_BW_LARGE =
      "application/vnd.3gpp.pic-bw-large";
  public static final String APPLICATION_VND_3GPP_PIC_BW_SMALL =
      "application/vnd.3gpp.pic-bw-small";
  public static final String APPLICATION_VND_3GPP_PIC_BW_VAR = "application/vnd.3gpp.pic-bw-var";
  public static final String APPLICATION_VND_3GPP2_TCAP = "application/vnd.3gpp2.tcap";
  public static final String APPLICATION_VND_3M_POST_IT_NOTES = "application/vnd.3m.post-it-notes";
  public static final String APPLICATION_VND_ACCPAC_SIMPLY_ASO =
      "application/vnd.accpac.simply.aso";
  public static final String APPLICATION_VND_ACCPAC_SIMPLY_IMP =
      "application/vnd.accpac.simply.imp";
  public static final String APPLICATION_VND_ACUCOBOL = "application/vnd.acucobol";
  public static final String APPLICATION_VND_ACUCORP = "application/vnd.acucorp";
  public static final String APPLICATION_VND_ADOBE_AIR_APPLICATION_INSTALLER_PACKAGE_ZIP =
      "application/vnd.adobe.air-application-installer-package+zip";
  public static final String APPLICATION_VND_ADOBE_FORMSCENTRAL_FCDT =
      "application/vnd.adobe.formscentral.fcdt";
  public static final String APPLICATION_VND_ADOBE_FXP = "application/vnd.adobe.fxp";
  public static final String APPLICATION_VND_ADOBE_XDP_XML = "application/vnd.adobe.xdp+xml";
  public static final String APPLICATION_VND_ADOBE_XFDF = "application/vnd.adobe.xfdf";
  public static final String APPLICATION_VND_AHEAD_SPACE = "application/vnd.ahead.space";
  public static final String APPLICATION_VND_AIRZIP_FILESECURE_AZF =
      "application/vnd.airzip.filesecure.azf";
  public static final String APPLICATION_VND_AIRZIP_FILESECURE_AZS =
      "application/vnd.airzip.filesecure.azs";
  public static final String APPLICATION_VND_AMAZON_EBOOK = "application/vnd.amazon.ebook";
  public static final String APPLICATION_VND_AMERICANDYNAMICS_ACC =
      "application/vnd.americandynamics.acc";
  public static final String APPLICATION_VND_AMIGA_AMI = "application/vnd.amiga.ami";
  public static final String APPLICATION_VND_ANDROID_PACKAGE_ARCHIVE =
      "application/vnd.android.package-archive";
  public static final String APPLICATION_VND_ANSER_WEB_CERTIFICATE_ISSUE_INITIATION =
      "application/vnd.anser-web-certificate-issue-initiation";
  public static final String APPLICATION_VND_ANSER_WEB_FUNDS_TRANSFER_INITIATION =
      "application/vnd.anser-web-funds-transfer-initiation";
  public static final String APPLICATION_VND_ANTIX_GAME_COMPONENT =
      "application/vnd.antix.game-component";
  public static final String APPLICATION_VND_APPLE_INSTALLER_XML =
      "application/vnd.apple.installer+xml";
  public static final String APPLICATION_VND_APPLE_MPEGURL = "application/vnd.apple.mpegurl";
  public static final String APPLICATION_VND_ARISTANETWORKS_SWI =
      "application/vnd.aristanetworks.swi";
  public static final String APPLICATION_VND_ASTRAEA_SOFTWARE_IOTA =
      "application/vnd.astraea-software.iota";
  public static final String APPLICATION_VND_AUDIOGRAPH = "application/vnd.audiograph";
  public static final String APPLICATION_VND_BLUEICE_MULTIPASS =
      "application/vnd.blueice.multipass";
  public static final String APPLICATION_VND_BMI = "application/vnd.bmi";
  public static final String APPLICATION_VND_BUSINESSOBJECTS = "application/vnd.businessobjects";
  public static final String APPLICATION_VND_CHEMDRAW_XML = "application/vnd.chemdraw+xml";
  public static final String APPLICATION_VND_CHIPNUTS_KARAOKE_MMD =
      "application/vnd.chipnuts.karaoke-mmd";
  public static final String APPLICATION_VND_CINDERELLA = "application/vnd.cinderella";
  public static final String APPLICATION_VND_CLAYMORE = "application/vnd.claymore";
  public static final String APPLICATION_VND_CLOANTO_RP9 = "application/vnd.cloanto.rp9";
  public static final String APPLICATION_VND_CLONK_C4GROUP = "application/vnd.clonk.c4group";
  public static final String APPLICATION_VND_CLUETRUST_CARTOMOBILE_CONFIG =
      "application/vnd.cluetrust.cartomobile-config";
  public static final String APPLICATION_VND_CLUETRUST_CARTOMOBILE_CONFIG_PKG =
      "application/vnd.cluetrust.cartomobile-config-pkg";
  public static final String APPLICATION_VND_COMMONSPACE = "application/vnd.commonspace";
  public static final String APPLICATION_VND_CONTACT_CMSG = "application/vnd.contact.cmsg";
  public static final String APPLICATION_VND_COSMOCALLER = "application/vnd.cosmocaller";
  public static final String APPLICATION_VND_CRICK_CLICKER = "application/vnd.crick.clicker";
  public static final String APPLICATION_VND_CRICK_CLICKER_KEYBOARD =
      "application/vnd.crick.clicker.keyboard";
  public static final String APPLICATION_VND_CRICK_CLICKER_PALETTE =
      "application/vnd.crick.clicker.palette";
  public static final String APPLICATION_VND_CRICK_CLICKER_TEMPLATE =
      "application/vnd.crick.clicker.template";
  public static final String APPLICATION_VND_CRICK_CLICKER_WORDBANK =
      "application/vnd.crick.clicker.wordbank";
  public static final String APPLICATION_VND_CRITICALTOOLS_WBS_XML =
      "application/vnd.criticaltools.wbs+xml";
  public static final String APPLICATION_VND_CTC_POSML = "application/vnd.ctc-posml";
  public static final String APPLICATION_VND_CUPS_PPD = "application/vnd.cups-ppd";
  public static final String APPLICATION_VND_CURL_CAR = "application/vnd.curl.car";
  public static final String APPLICATION_VND_CURL_PCURL = "application/vnd.curl.pcurl";
  public static final String APPLICATION_VND_DART = "application/vnd.dart";
  public static final String APPLICATION_VND_DATA_VISION_RDZ = "application/vnd.data-vision.rdz";
  public static final String APPLICATION_VND_DECE_DATA = "application/vnd.dece.data";
  public static final String APPLICATION_VND_DECE_TTML_XML = "application/vnd.dece.ttml+xml";
  public static final String APPLICATION_VND_DECE_UNSPECIFIED = "application/vnd.dece.unspecified";
  public static final String APPLICATION_VND_DECE_ZIP = "application/vnd.dece.zip";
  public static final String APPLICATION_VND_DENOVO_FCSELAYOUT_LINK =
      "application/vnd.denovo.fcselayout-link";
  public static final String APPLICATION_VND_DNA = "application/vnd.dna";
  public static final String APPLICATION_VND_DOLBY_MLP = "application/vnd.dolby.mlp";
  public static final String APPLICATION_VND_DPGRAPH = "application/vnd.dpgraph";
  public static final String APPLICATION_VND_DREAMFACTORY = "application/vnd.dreamfactory";
  public static final String APPLICATION_VND_DS_KEYPOINT = "application/vnd.ds-keypoint";
  public static final String APPLICATION_VND_DVB_AIT = "application/vnd.dvb.ait";
  public static final String APPLICATION_VND_DVB_SERVICE = "application/vnd.dvb.service";
  public static final String APPLICATION_VND_DYNAGEO = "application/vnd.dynageo";
  public static final String APPLICATION_VND_ECOWIN_CHART = "application/vnd.ecowin.chart";
  public static final String APPLICATION_VND_ENLIVEN = "application/vnd.enliven";
  public static final String APPLICATION_VND_EPSON_ESF = "application/vnd.epson.esf";
  public static final String APPLICATION_VND_EPSON_MSF = "application/vnd.epson.msf";
  public static final String APPLICATION_VND_EPSON_QUICKANIME = "application/vnd.epson.quickanime";
  public static final String APPLICATION_VND_EPSON_SALT = "application/vnd.epson.salt";
  public static final String APPLICATION_VND_EPSON_SSF = "application/vnd.epson.ssf";
  public static final String APPLICATION_VND_ESZIGNO3_XML = "application/vnd.eszigno3+xml";
  public static final String APPLICATION_VND_EZPIX_ALBUM = "application/vnd.ezpix-album";
  public static final String APPLICATION_VND_EZPIX_PACKAGE = "application/vnd.ezpix-package";
  public static final String APPLICATION_VND_FDF = "application/vnd.fdf";
  public static final String APPLICATION_VND_FDSN_MSEED = "application/vnd.fdsn.mseed";
  public static final String APPLICATION_VND_FDSN_SEED = "application/vnd.fdsn.seed";
  public static final String APPLICATION_VND_FLOGRAPHIT = "application/vnd.flographit";
  public static final String APPLICATION_VND_FLUXTIME_CLIP = "application/vnd.fluxtime.clip";
  public static final String APPLICATION_VND_FRAMEMAKER = "application/vnd.framemaker";
  public static final String APPLICATION_VND_FROGANS_FNC = "application/vnd.frogans.fnc";
  public static final String APPLICATION_VND_FROGANS_LTF = "application/vnd.frogans.ltf";
  public static final String APPLICATION_VND_FSC_WEBLAUNCH = "application/vnd.fsc.weblaunch";
  public static final String APPLICATION_VND_FUJITSU_OASYS = "application/vnd.fujitsu.oasys";
  public static final String APPLICATION_VND_FUJITSU_OASYS2 = "application/vnd.fujitsu.oasys2";
  public static final String APPLICATION_VND_FUJITSU_OASYS3 = "application/vnd.fujitsu.oasys3";
  public static final String APPLICATION_VND_FUJITSU_OASYSGP = "application/vnd.fujitsu.oasysgp";
  public static final String APPLICATION_VND_FUJITSU_OASYSPRS = "application/vnd.fujitsu.oasysprs";
  public static final String APPLICATION_VND_FUJIXEROX_DDD = "application/vnd.fujixerox.ddd";
  public static final String APPLICATION_VND_FUJIXEROX_DOCUWORKS =
      "application/vnd.fujixerox.docuworks";
  public static final String APPLICATION_VND_FUJIXEROX_DOCUWORKS_BINDER =
      "application/vnd.fujixerox.docuworks.binder";
  public static final String APPLICATION_VND_FUZZYSHEET = "application/vnd.fuzzysheet";
  public static final String APPLICATION_VND_GENOMATIX_TUXEDO = "application/vnd.genomatix.tuxedo";
  public static final String APPLICATION_VND_GEOGEBRA_FILE = "application/vnd.geogebra.file";
  public static final String APPLICATION_VND_GEOGEBRA_TOOL = "application/vnd.geogebra.tool";
  public static final String APPLICATION_VND_GEOMETRY_EXPLORER =
      "application/vnd.geometry-explorer";
  public static final String APPLICATION_VND_GEONEXT = "application/vnd.geonext";
  public static final String APPLICATION_VND_GEOPLAN = "application/vnd.geoplan";
  public static final String APPLICATION_VND_GEOSPACE = "application/vnd.geospace";
  public static final String APPLICATION_VND_GMX = "application/vnd.gmx";
  public static final String APPLICATION_VND_GOOGLE_EARTH_KML_XML =
      "application/vnd.google-earth.kml+xml";
  public static final String APPLICATION_VND_GOOGLE_EARTH_KMZ = "application/vnd.google-earth.kmz";
  public static final String APPLICATION_VND_GRAFEQ = "application/vnd.grafeq";
  public static final String APPLICATION_VND_GROOVE_ACCOUNT = "application/vnd.groove-account";
  public static final String APPLICATION_VND_GROOVE_HELP = "application/vnd.groove-help";
  public static final String APPLICATION_VND_GROOVE_IDENTITY_MESSAGE =
      "application/vnd.groove-identity-message";
  public static final String APPLICATION_VND_GROOVE_INJECTOR = "application/vnd.groove-injector";
  public static final String APPLICATION_VND_GROOVE_TOOL_MESSAGE =
      "application/vnd.groove-tool-message";
  public static final String APPLICATION_VND_GROOVE_TOOL_TEMPLATE =
      "application/vnd.groove-tool-template";
  public static final String APPLICATION_VND_GROOVE_VCARD = "application/vnd.groove-vcard";
  public static final String APPLICATION_VND_HAL_XML = "application/vnd.hal+xml";
  public static final String APPLICATION_VND_HANDHELD_ENTERTAINMENT_XML =
      "application/vnd.handheld-entertainment+xml";
  public static final String APPLICATION_VND_HBCI = "application/vnd.hbci";
  public static final String APPLICATION_VND_HHE_LESSON_PLAYER =
      "application/vnd.hhe.lesson-player";
  public static final String APPLICATION_VND_HP_HPGL = "application/vnd.hp-hpgl";
  public static final String APPLICATION_VND_HP_HPID = "application/vnd.hp-hpid";
  public static final String APPLICATION_VND_HP_HPS = "application/vnd.hp-hps";
  public static final String APPLICATION_VND_HP_JLYT = "application/vnd.hp-jlyt";
  public static final String APPLICATION_VND_HP_PCL = "application/vnd.hp-pcl";
  public static final String APPLICATION_VND_HP_PCLXL = "application/vnd.hp-pclxl";
  public static final String APPLICATION_VND_HYDROSTATIX_SOF_DATA =
      "application/vnd.hydrostatix.sof-data";
  public static final String APPLICATION_VND_IBM_MINIPAY = "application/vnd.ibm.minipay";
  public static final String APPLICATION_VND_IBM_MODCAP = "application/vnd.ibm.modcap";
  public static final String APPLICATION_VND_IBM_RIGHTS_MANAGEMENT =
      "application/vnd.ibm.rights-management";
  public static final String APPLICATION_VND_IBM_SECURE_CONTAINER =
      "application/vnd.ibm.secure-container";
  public static final String APPLICATION_VND_ICCPROFILE = "application/vnd.iccprofile";
  public static final String APPLICATION_VND_IGLOADER = "application/vnd.igloader";
  public static final String APPLICATION_VND_IMMERVISION_IVP = "application/vnd.immervision-ivp";
  public static final String APPLICATION_VND_IMMERVISION_IVU = "application/vnd.immervision-ivu";
  public static final String APPLICATION_VND_INSORS_IGM = "application/vnd.insors.igm";
  public static final String APPLICATION_VND_INTERCON_FORMNET = "application/vnd.intercon.formnet";
  public static final String APPLICATION_VND_INTERGEO = "application/vnd.intergeo";
  public static final String APPLICATION_VND_INTU_QBO = "application/vnd.intu.qbo";
  public static final String APPLICATION_VND_INTU_QFX = "application/vnd.intu.qfx";
  public static final String APPLICATION_VND_IPUNPLUGGED_RCPROFILE =
      "application/vnd.ipunplugged.rcprofile";
  public static final String APPLICATION_VND_IREPOSITORY_PACKAGE_XML =
      "application/vnd.irepository.package+xml";
  public static final String APPLICATION_VND_IS_XPR = "application/vnd.is-xpr";
  public static final String APPLICATION_VND_ISAC_FCS = "application/vnd.isac.fcs";
  public static final String APPLICATION_VND_JAM = "application/vnd.jam";
  public static final String APPLICATION_VND_JCP_JAVAME_MIDLET_RMS =
      "application/vnd.jcp.javame.midlet-rms";
  public static final String APPLICATION_VND_JISP = "application/vnd.jisp";
  public static final String APPLICATION_VND_JOOST_JODA_ARCHIVE =
      "application/vnd.joost.joda-archive";
  public static final String APPLICATION_VND_KAHOOTZ = "application/vnd.kahootz";
  public static final String APPLICATION_VND_KDE_KARBON = "application/vnd.kde.karbon";
  public static final String APPLICATION_VND_KDE_KCHART = "application/vnd.kde.kchart";
  public static final String APPLICATION_VND_KDE_KFORMULA = "application/vnd.kde.kformula";
  public static final String APPLICATION_VND_KDE_KIVIO = "application/vnd.kde.kivio";
  public static final String APPLICATION_VND_KDE_KONTOUR = "application/vnd.kde.kontour";
  public static final String APPLICATION_VND_KDE_KPRESENTER = "application/vnd.kde.kpresenter";
  public static final String APPLICATION_VND_KDE_KSPREAD = "application/vnd.kde.kspread";
  public static final String APPLICATION_VND_KDE_KWORD = "application/vnd.kde.kword";
  public static final String APPLICATION_VND_KENAMEAAPP = "application/vnd.kenameaapp";
  public static final String APPLICATION_VND_KIDSPIRATION = "application/vnd.kidspiration";
  public static final String APPLICATION_VND_KINAR = "application/vnd.kinar";
  public static final String APPLICATION_VND_KOAN = "application/vnd.koan";
  public static final String APPLICATION_VND_KODAK_DESCRIPTOR = "application/vnd.kodak-descriptor";
  public static final String APPLICATION_VND_LAS_LAS_XML = "application/vnd.las.las+xml";
  public static final String APPLICATION_VND_LLAMAGRAPHICS_LIFE_BALANCE_DESKTOP =
      "application/vnd.llamagraphics.life-balance.desktop";
  public static final String APPLICATION_VND_LLAMAGRAPHICS_LIFE_BALANCE_EXCHANGE_XML =
      "application/vnd.llamagraphics.life-balance.exchange+xml";
  public static final String APPLICATION_VND_LOTUS_1_2_3 = "application/vnd.lotus-1-2-3";
  public static final String APPLICATION_VND_LOTUS_APPROACH = "application/vnd.lotus-approach";
  public static final String APPLICATION_VND_LOTUS_FREELANCE = "application/vnd.lotus-freelance";
  public static final String APPLICATION_VND_LOTUS_NOTES = "application/vnd.lotus-notes";
  public static final String APPLICATION_VND_LOTUS_ORGANIZER = "application/vnd.lotus-organizer";
  public static final String APPLICATION_VND_LOTUS_SCREENCAM = "application/vnd.lotus-screencam";
  public static final String APPLICATION_VND_LOTUS_WORDPRO = "application/vnd.lotus-wordpro";
  public static final String APPLICATION_VND_MACPORTS_PORTPKG = "application/vnd.macports.portpkg";
  public static final String APPLICATION_VND_MCD = "application/vnd.mcd";
  public static final String APPLICATION_VND_MEDCALCDATA = "application/vnd.medcalcdata";
  public static final String APPLICATION_VND_MEDIASTATION_CDKEY =
      "application/vnd.mediastation.cdkey";
  public static final String APPLICATION_VND_MFER = "application/vnd.mfer";
  public static final String APPLICATION_VND_MFMP = "application/vnd.mfmp";
  public static final String APPLICATION_VND_MICROGRAFX_FLO = "application/vnd.micrografx.flo";
  public static final String APPLICATION_VND_MICROGRAFX_IGX = "application/vnd.micrografx.igx";
  public static final String APPLICATION_VND_MIF = "application/vnd.mif";
  public static final String APPLICATION_VND_MOBIUS_DAF = "application/vnd.mobius.daf";
  public static final String APPLICATION_VND_MOBIUS_DIS = "application/vnd.mobius.dis";
  public static final String APPLICATION_VND_MOBIUS_MBK = "application/vnd.mobius.mbk";
  public static final String APPLICATION_VND_MOBIUS_MQY = "application/vnd.mobius.mqy";
  public static final String APPLICATION_VND_MOBIUS_MSL = "application/vnd.mobius.msl";
  public static final String APPLICATION_VND_MOBIUS_PLC = "application/vnd.mobius.plc";
  public static final String APPLICATION_VND_MOBIUS_TXF = "application/vnd.mobius.txf";
  public static final String APPLICATION_VND_MOPHUN_APPLICATION =
      "application/vnd.mophun.application";
  public static final String APPLICATION_VND_MOPHUN_CERTIFICATE =
      "application/vnd.mophun.certificate";
  public static final String APPLICATION_VND_MOZILLA_XUL_XML = "application/vnd.mozilla.xul+xml";
  public static final String APPLICATION_VND_MS_ARTGALRY = "application/vnd.ms-artgalry";
  public static final String APPLICATION_VND_MS_CAB_COMPRESSED =
      "application/vnd.ms-cab-compressed";
  public static final String APPLICATION_VND_MS_EXCEL = "application/vnd.ms-excel";
  public static final String APPLICATION_VND_MS_EXCEL_ADDIN_MACROENABLED_12 =
      "application/vnd.ms-excel.addin.macroenabled.12";
  public static final String APPLICATION_VND_MS_EXCEL_SHEET_BINARY_MACROENABLED_12 =
      "application/vnd.ms-excel.sheet.binary.macroenabled.12";
  public static final String APPLICATION_VND_MS_EXCEL_SHEET_MACROENABLED_12 =
      "application/vnd.ms-excel.sheet.macroenabled.12";
  public static final String APPLICATION_VND_MS_EXCEL_TEMPLATE_MACROENABLED_12 =
      "application/vnd.ms-excel.template.macroenabled.12";
  public static final String APPLICATION_VND_MS_FONTOBJECT = "application/vnd.ms-fontobject";
  public static final String APPLICATION_VND_MS_HTMLHELP = "application/vnd.ms-htmlhelp";
  public static final String APPLICATION_VND_MS_IMS = "application/vnd.ms-ims";
  public static final String APPLICATION_VND_MS_LRM = "application/vnd.ms-lrm";
  public static final String APPLICATION_VND_MS_OFFICETHEME = "application/vnd.ms-officetheme";
  public static final String APPLICATION_VND_MS_PKI_SECCAT = "application/vnd.ms-pki.seccat";
  public static final String APPLICATION_VND_MS_PKI_STL = "application/vnd.ms-pki.stl";
  public static final String APPLICATION_VND_MS_POWERPOINT = "application/vnd.ms-powerpoint";
  public static final String APPLICATION_VND_MS_POWERPOINT_ADDIN_MACROENABLED_12 =
      "application/vnd.ms-powerpoint.addin.macroenabled.12";
  public static final String APPLICATION_VND_MS_POWERPOINT_PRESENTATION_MACROENABLED_12 =
      "application/vnd.ms-powerpoint.presentation.macroenabled.12";
  public static final String APPLICATION_VND_MS_POWERPOINT_SLIDE_MACROENABLED_12 =
      "application/vnd.ms-powerpoint.slide.macroenabled.12";
  public static final String APPLICATION_VND_MS_POWERPOINT_SLIDESHOW_MACROENABLED_12 =
      "application/vnd.ms-powerpoint.slideshow.macroenabled.12";
  public static final String APPLICATION_VND_MS_POWERPOINT_TEMPLATE_MACROENABLED_12 =
      "application/vnd.ms-powerpoint.template.macroenabled.12";
  public static final String APPLICATION_VND_MS_PROJECT = "application/vnd.ms-project";

  public static final String APPLICATION_VND_MS_WORD_DOCUMENT_MACROENABLED_12 =
      "application/vnd.ms-word.document.macroenabled.12";
  public static final String APPLICATION_VND_MS_WORD_TEMPLATE_MACROENABLED_12 =
      "application/vnd.ms-word.template.macroenabled.12";
  public static final String APPLICATION_VND_MS_WORKS = "application/vnd.ms-works";
  public static final String APPLICATION_VND_MS_WPL = "application/vnd.ms-wpl";
  public static final String APPLICATION_VND_MS_XPSDOCUMENT = "application/vnd.ms-xpsdocument";
  public static final String APPLICATION_VND_MSEQ = "application/vnd.mseq";
  public static final String APPLICATION_VND_MUSICIAN = "application/vnd.musician";
  public static final String APPLICATION_VND_MUVEE_STYLE = "application/vnd.muvee.style";
  public static final String APPLICATION_VND_MYNFC = "application/vnd.mynfc";
  public static final String APPLICATION_VND_NEUROLANGUAGE_NLU =
      "application/vnd.neurolanguage.nlu";
  public static final String APPLICATION_VND_NITF = "application/vnd.nitf";
  public static final String APPLICATION_VND_NOBLENET_DIRECTORY =
      "application/vnd.noblenet-directory";
  public static final String APPLICATION_VND_NOBLENET_SEALER = "application/vnd.noblenet-sealer";
  public static final String APPLICATION_VND_NOBLENET_WEB = "application/vnd.noblenet-web";
  public static final String APPLICATION_VND_NOKIA_N_GAGE_DATA =
      "application/vnd.nokia.n-gage.data";
  public static final String APPLICATION_VND_NOKIA_N_GAGE_SYMBIAN_INSTALL =
      "application/vnd.nokia.n-gage.symbian.install";
  public static final String APPLICATION_VND_NOKIA_RADIO_PRESET =
      "application/vnd.nokia.radio-preset";
  public static final String APPLICATION_VND_NOKIA_RADIO_PRESETS =
      "application/vnd.nokia.radio-presets";
  public static final String APPLICATION_VND_NOVADIGM_EDM = "application/vnd.novadigm.edm";
  public static final String APPLICATION_VND_NOVADIGM_EDX = "application/vnd.novadigm.edx";
  public static final String APPLICATION_VND_NOVADIGM_EXT = "application/vnd.novadigm.ext";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_CHART =
      "application/vnd.oasis.opendocument.chart";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_CHART_TEMPLATE =
      "application/vnd.oasis.opendocument.chart-template";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_DATABASE =
      "application/vnd.oasis.opendocument.database";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_FORMULA =
      "application/vnd.oasis.opendocument.formula";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_FORMULA_TEMPLATE =
      "application/vnd.oasis.opendocument.formula-template";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_GRAPHICS =
      "application/vnd.oasis.opendocument.graphics";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_GRAPHICS_TEMPLATE =
      "application/vnd.oasis.opendocument.graphics-template";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_IMAGE =
      "application/vnd.oasis.opendocument.image";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_IMAGE_TEMPLATE =
      "application/vnd.oasis.opendocument.image-template";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_PRESENTATION =
      "application/vnd.oasis.opendocument.presentation";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_PRESENTATION_TEMPLATE =
      "application/vnd.oasis.opendocument.presentation-template";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_SPREADSHEET =
      "application/vnd.oasis.opendocument.spreadsheet";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_SPREADSHEET_TEMPLATE =
      "application/vnd.oasis.opendocument.spreadsheet-template";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_TEXT =
      "application/vnd.oasis.opendocument.text";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_TEXT_MASTER =
      "application/vnd.oasis.opendocument.text-master";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_TEXT_TEMPLATE =
      "application/vnd.oasis.opendocument.text-template";
  public static final String APPLICATION_VND_OASIS_OPENDOCUMENT_TEXT_WEB =
      "application/vnd.oasis.opendocument.text-web";
  public static final String APPLICATION_VND_OLPC_SUGAR = "application/vnd.olpc-sugar";
  public static final String APPLICATION_VND_OMA_DD2_XML = "application/vnd.oma.dd2+xml";
  public static final String APPLICATION_VND_OPENOFFICEORG_EXTENSION =
      "application/vnd.openofficeorg.extension";
  public static final String
      APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_PRESENTATION =
      "application/vnd.openxmlformats-officedocument.presentationml.presentation";
  public static final String APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_SLIDE =
      "application/vnd.openxmlformats-officedocument.presentationml.slide";
  public static final String
      APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_SLIDESHOW =
      "application/vnd.openxmlformats-officedocument.presentationml.slideshow";
  public static final String APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_TEMPLATE =
      "application/vnd.openxmlformats-officedocument.presentationml.template";
  public static final String APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_SPREADSHEETML_SHEET =
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  public static final String APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_SPREADSHEETML_TEMPLATE =
      "application/vnd.openxmlformats-officedocument.spreadsheetml.template";
  public static final String
      APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_WORDPROCESSINGML_DOCUMENT =
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  public static final String
      APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_WORDPROCESSINGML_TEMPLATE =
      "application/vnd.openxmlformats-officedocument.wordprocessingml.template";
  public static final String APPLICATION_VND_OSGEO_MAPGUIDE_PACKAGE =
      "application/vnd.osgeo.mapguide.package";
  public static final String APPLICATION_VND_OSGI_DP = "application/vnd.osgi.dp";
  public static final String APPLICATION_VND_OSGI_SUBSYSTEM = "application/vnd.osgi.subsystem";
  public static final String APPLICATION_VND_PALM = "application/vnd.palm";
  public static final String APPLICATION_VND_PAWAAFILE = "application/vnd.pawaafile";
  public static final String APPLICATION_VND_PG_FORMAT = "application/vnd.pg.format";
  public static final String APPLICATION_VND_PG_OSASLI = "application/vnd.pg.osasli";
  public static final String APPLICATION_VND_PICSEL = "application/vnd.picsel";
  public static final String APPLICATION_VND_PMI_WIDGET = "application/vnd.pmi.widget";
  public static final String APPLICATION_VND_POCKETLEARN = "application/vnd.pocketlearn";
  public static final String APPLICATION_VND_POWERBUILDER6 = "application/vnd.powerbuilder6";
  public static final String APPLICATION_VND_PREVIEWSYSTEMS_BOX =
      "application/vnd.previewsystems.box";
  public static final String APPLICATION_VND_PROTEUS_MAGAZINE = "application/vnd.proteus.magazine";
  public static final String APPLICATION_VND_PUBLISHARE_DELTA_TREE =
      "application/vnd.publishare-delta-tree";
  public static final String APPLICATION_VND_PVI_PTID1 = "application/vnd.pvi.ptid1";
  public static final String APPLICATION_VND_QUARK_QUARKXPRESS =
      "application/vnd.quark.quarkxpress";
  public static final String APPLICATION_VND_REALVNC_BED = "application/vnd.realvnc.bed";
  public static final String APPLICATION_VND_RECORDARE_MUSICXML =
      "application/vnd.recordare.musicxml";
  public static final String APPLICATION_VND_RECORDARE_MUSICXML_XML =
      "application/vnd.recordare.musicxml+xml";
  public static final String APPLICATION_VND_RIG_CRYPTONOTE = "application/vnd.rig.cryptonote";
  public static final String APPLICATION_VND_RIM_COD = "application/vnd.rim.cod";
  public static final String APPLICATION_VND_RN_REALMEDIA = "application/vnd.rn-realmedia";
  public static final String APPLICATION_VND_RN_REALMEDIA_VBR = "application/vnd.rn-realmedia-vbr";
  public static final String APPLICATION_VND_ROUTE66_LINK66_XML =
      "application/vnd.route66.link66+xml";
  public static final String APPLICATION_VND_SAILINGTRACKER_TRACK =
      "application/vnd.sailingtracker.track";
  public static final String APPLICATION_VND_SEEMAIL = "application/vnd.seemail";
  public static final String APPLICATION_VND_SEMA = "application/vnd.sema";
  public static final String APPLICATION_VND_SEMD = "application/vnd.semd";
  public static final String APPLICATION_VND_SEMF = "application/vnd.semf";
  public static final String APPLICATION_VND_SHANA_INFORMED_FORMDATA =
      "application/vnd.shana.informed.formdata";
  public static final String APPLICATION_VND_SHANA_INFORMED_FORMTEMPLATE =
      "application/vnd.shana.informed.formtemplate";
  public static final String APPLICATION_VND_SHANA_INFORMED_INTERCHANGE =
      "application/vnd.shana.informed.interchange";
  public static final String APPLICATION_VND_SHANA_INFORMED_PACKAGE =
      "application/vnd.shana.informed.package";
  public static final String APPLICATION_VND_SIMTECH_MINDMAPPER =
      "application/vnd.simtech-mindmapper";
  public static final String APPLICATION_VND_SMAF = "application/vnd.smaf";
  public static final String APPLICATION_VND_SMART_TEACHER = "application/vnd.smart.teacher";
  public static final String APPLICATION_VND_SOLENT_SDKM_XML = "application/vnd.solent.sdkm+xml";
  public static final String APPLICATION_VND_SPOTFIRE_DXP = "application/vnd.spotfire.dxp";
  public static final String APPLICATION_VND_SPOTFIRE_SFS = "application/vnd.spotfire.sfs";
  public static final String APPLICATION_VND_STARDIVISION_CALC =
      "application/vnd.stardivision.calc";
  public static final String APPLICATION_VND_STARDIVISION_DRAW =
      "application/vnd.stardivision.draw";
  public static final String APPLICATION_VND_STARDIVISION_IMPRESS =
      "application/vnd.stardivision.impress";
  public static final String APPLICATION_VND_STARDIVISION_MATH =
      "application/vnd.stardivision.math";
  public static final String APPLICATION_VND_STARDIVISION_WRITER =
      "application/vnd.stardivision.writer";
  public static final String APPLICATION_VND_STARDIVISION_WRITER_GLOBAL =
      "application/vnd.stardivision.writer-global";
  public static final String APPLICATION_VND_STEPMANIA_PACKAGE =
      "application/vnd.stepmania.package";
  public static final String APPLICATION_VND_STEPMANIA_STEPCHART =
      "application/vnd.stepmania.stepchart";
  public static final String APPLICATION_VND_SUN_XML_CALC = "application/vnd.sun.xml.calc";
  public static final String APPLICATION_VND_SUN_XML_CALC_TEMPLATE =
      "application/vnd.sun.xml.calc.template";
  public static final String APPLICATION_VND_SUN_XML_DRAW = "application/vnd.sun.xml.draw";
  public static final String APPLICATION_VND_SUN_XML_DRAW_TEMPLATE =
      "application/vnd.sun.xml.draw.template";
  public static final String APPLICATION_VND_SUN_XML_IMPRESS = "application/vnd.sun.xml.impress";
  public static final String APPLICATION_VND_SUN_XML_IMPRESS_TEMPLATE =
      "application/vnd.sun.xml.impress.template";
  public static final String APPLICATION_VND_SUN_XML_MATH = "application/vnd.sun.xml.math";
  public static final String APPLICATION_VND_SUN_XML_WRITER = "application/vnd.sun.xml.writer";
  public static final String APPLICATION_VND_SUN_XML_WRITER_GLOBAL =
      "application/vnd.sun.xml.writer.global";
  public static final String APPLICATION_VND_SUN_XML_WRITER_TEMPLATE =
      "application/vnd.sun.xml.writer.template";
  public static final String APPLICATION_VND_SUS_CALENDAR = "application/vnd.sus-calendar";
  public static final String APPLICATION_VND_SVD = "application/vnd.svd";
  public static final String APPLICATION_VND_SYMBIAN_INSTALL = "application/vnd.symbian.install";
  public static final String APPLICATION_VND_SYNCML_XML = "application/vnd.syncml+xml";
  public static final String APPLICATION_VND_SYNCML_DM_WBXML = "application/vnd.syncml.dm+wbxml";
  public static final String APPLICATION_VND_SYNCML_DM_XML = "application/vnd.syncml.dm+xml";
  public static final String APPLICATION_VND_TAO_INTENT_MODULE_ARCHIVE =
      "application/vnd.tao.intent-module-archive";
  public static final String APPLICATION_VND_TCPDUMP_PCAP = "application/vnd.tcpdump.pcap";
  public static final String APPLICATION_VND_TMOBILE_LIVETV = "application/vnd.tmobile-livetv";
  public static final String APPLICATION_VND_TRID_TPT = "application/vnd.trid.tpt";
  public static final String APPLICATION_VND_TRISCAPE_MXS = "application/vnd.triscape.mxs";
  public static final String APPLICATION_VND_TRUEAPP = "application/vnd.trueapp";
  public static final String APPLICATION_VND_UFDL = "application/vnd.ufdl";
  public static final String APPLICATION_VND_UIQ_THEME = "application/vnd.uiq.theme";
  public static final String APPLICATION_VND_UMAJIN = "application/vnd.umajin";
  public static final String APPLICATION_VND_UNITY = "application/vnd.unity";
  public static final String APPLICATION_VND_UOML_XML = "application/vnd.uoml+xml";
  public static final String APPLICATION_VND_VCX = "application/vnd.vcx";
  public static final String APPLICATION_VND_VISIO = "application/vnd.visio";
  public static final String APPLICATION_VND_VISIONARY = "application/vnd.visionary";
  public static final String APPLICATION_VND_VSF = "application/vnd.vsf";
  public static final String APPLICATION_VND_WAP_WBXML = "application/vnd.wap.wbxml";
  public static final String APPLICATION_VND_WAP_WMLC = "application/vnd.wap.wmlc";
  public static final String APPLICATION_VND_WAP_WMLSCRIPTC = "application/vnd.wap.wmlscriptc";
  public static final String APPLICATION_VND_WEBTURBO = "application/vnd.webturbo";
  public static final String APPLICATION_VND_WOLFRAM_PLAYER = "application/vnd.wolfram.player";
  public static final String APPLICATION_VND_WORDPERFECT = "application/vnd.wordperfect";
  public static final String APPLICATION_VND_WQD = "application/vnd.wqd";
  public static final String APPLICATION_VND_WT_STF = "application/vnd.wt.stf";
  public static final String APPLICATION_VND_XARA = "application/vnd.xara";
  public static final String APPLICATION_VND_XFDL = "application/vnd.xfdl";
  public static final String APPLICATION_VND_YAMAHA_HV_DIC = "application/vnd.yamaha.hv-dic";
  public static final String APPLICATION_VND_YAMAHA_HV_SCRIPT = "application/vnd.yamaha.hv-script";
  public static final String APPLICATION_VND_YAMAHA_HV_VOICE = "application/vnd.yamaha.hv-voice";
  public static final String APPLICATION_VND_YAMAHA_OPENSCOREFORMAT =
      "application/vnd.yamaha.openscoreformat";
  public static final String APPLICATION_VND_YAMAHA_OPENSCOREFORMAT_OSFPVG_XML =
      "application/vnd.yamaha.openscoreformat.osfpvg+xml";
  public static final String APPLICATION_VND_YAMAHA_SMAF_AUDIO =
      "application/vnd.yamaha.smaf-audio";
  public static final String APPLICATION_VND_YAMAHA_SMAF_PHRASE =
      "application/vnd.yamaha.smaf-phrase";
  public static final String APPLICATION_VND_YELLOWRIVER_CUSTOM_MENU =
      "application/vnd.yellowriver-custom-menu";
  public static final String APPLICATION_VND_ZUL = "application/vnd.zul";
  public static final String APPLICATION_VND_ZZAZZ_DECK_XML = "application/vnd.zzazz.deck+xml";
  public static final String APPLICATION_VOICEXML_XML = "application/voicexml+xml";
  public static final String APPLICATION_WIDGET = "application/widget";
  public static final String APPLICATION_WINHLP = "application/winhlp";
  public static final String APPLICATION_WSDL_XML = "application/wsdl+xml";
  public static final String APPLICATION_WSPOLICY_XML = "application/wspolicy+xml";
  public static final String APPLICATION_X_7Z_COMPRESSED = "application/x-7z-compressed";
  public static final String APPLICATION_X_ABIWORD = "application/x-abiword";
  public static final String APPLICATION_X_ACE_COMPRESSED = "application/x-ace-compressed";
  public static final String APPLICATION_X_APPLE_DISKIMAGE = "application/x-apple-diskimage";
  public static final String APPLICATION_X_AUTHORWARE_BIN = "application/x-authorware-bin";
  public static final String APPLICATION_X_AUTHORWARE_MAP = "application/x-authorware-map";
  public static final String APPLICATION_X_AUTHORWARE_SEG = "application/x-authorware-seg";
  public static final String APPLICATION_X_BCPIO = "application/x-bcpio";
  public static final String APPLICATION_X_BITTORRENT = "application/x-bittorrent";
  public static final String APPLICATION_X_BLORB = "application/x-blorb";
  public static final String APPLICATION_X_BZIP = "application/x-bzip";
  public static final String APPLICATION_X_BZIP2 = "application/x-bzip2";
  public static final String APPLICATION_X_CBR = "application/x-cbr";
  public static final String APPLICATION_X_CDLINK = "application/x-cdlink";
  public static final String APPLICATION_X_CFS_COMPRESSED = "application/x-cfs-compressed";
  public static final String APPLICATION_X_CHAT = "application/x-chat";
  public static final String APPLICATION_X_CHESS_PGN = "application/x-chess-pgn";
  public static final String APPLICATION_X_CONFERENCE = "application/x-conference";
  public static final String APPLICATION_X_CPIO = "application/x-cpio";
  public static final String APPLICATION_X_CSH = "application/x-csh";
  public static final String APPLICATION_X_DEBIAN_PACKAGE = "application/x-debian-package";
  public static final String APPLICATION_X_DGC_COMPRESSED = "application/x-dgc-compressed";
  public static final String APPLICATION_X_DIRECTOR = "application/x-director";
  public static final String APPLICATION_X_DOOM = "application/x-doom";
  public static final String APPLICATION_X_DTBNCX_XML = "application/x-dtbncx+xml";
  public static final String APPLICATION_X_DTBOOK_XML = "application/x-dtbook+xml";
  public static final String APPLICATION_X_DTBRESOURCE_XML = "application/x-dtbresource+xml";
  public static final String APPLICATION_X_DVI = "application/x-dvi";
  public static final String APPLICATION_X_ENVOY = "application/x-envoy";
  public static final String APPLICATION_X_EVA = "application/x-eva";
  public static final String APPLICATION_X_FONT_BDF = "application/x-font-bdf";
  public static final String APPLICATION_X_FONT_GHOSTSCRIPT = "application/x-font-ghostscript";
  public static final String APPLICATION_X_FONT_LINUX_PSF = "application/x-font-linux-psf";
  public static final String APPLICATION_X_FONT_OTF = "application/x-font-otf";
  public static final String APPLICATION_X_FONT_PCF = "application/x-font-pcf";
  public static final String APPLICATION_X_FONT_SNF = "application/x-font-snf";
  public static final String APPLICATION_X_FONT_TTF = "application/x-font-ttf";
  public static final String APPLICATION_X_FONT_TYPE1 = "application/x-font-type1";
  public static final String APPLICATION_X_FONT_WOFF = "application/x-font-woff";
  public static final String APPLICATION_X_FREEARC = "application/x-freearc";
  public static final String APPLICATION_X_FUTURESPLASH = "application/x-futuresplash";
  public static final String APPLICATION_X_GCA_COMPRESSED = "application/x-gca-compressed";
  public static final String APPLICATION_X_GLULX = "application/x-glulx";
  public static final String APPLICATION_X_GNUMERIC = "application/x-gnumeric";
  public static final String APPLICATION_X_GRAMPS_XML = "application/x-gramps-xml";
  public static final String APPLICATION_X_GTAR = "application/x-gtar";
  public static final String APPLICATION_X_HDF = "application/x-hdf";
  public static final String APPLICATION_X_INSTALL_INSTRUCTIONS =
      "application/x-install-instructions";
  public static final String APPLICATION_X_ISO9660_IMAGE = "application/x-iso9660-image";
  public static final String APPLICATION_X_JAVA_JNLP_FILE = "application/x-java-jnlp-file";
  public static final String APPLICATION_X_LATEX = "application/x-latex";
  public static final String APPLICATION_X_LZH_COMPRESSED = "application/x-lzh-compressed";
  public static final String APPLICATION_X_MIE = "application/x-mie";
  public static final String APPLICATION_X_MOBIPOCKET_EBOOK = "application/x-mobipocket-ebook";
  public static final String APPLICATION_X_MS_APPLICATION = "application/x-ms-application";
  public static final String APPLICATION_X_MS_SHORTCUT = "application/x-ms-shortcut";
  public static final String APPLICATION_X_MS_WMD = "application/x-ms-wmd";
  public static final String APPLICATION_X_MS_WMZ = "application/x-ms-wmz";
  public static final String APPLICATION_X_MS_XBAP = "application/x-ms-xbap";
  public static final String APPLICATION_X_MSACCESS = "application/x-msaccess";
  public static final String APPLICATION_X_MSBINDER = "application/x-msbinder";
  public static final String APPLICATION_X_MSCARDFILE = "application/x-mscardfile";
  public static final String APPLICATION_X_MSCLIP = "application/x-msclip";
  public static final String APPLICATION_X_MSDOWNLOAD = "application/x-msdownload";
  public static final String APPLICATION_X_MSMEDIAVIEW = "application/x-msmediaview";
  public static final String APPLICATION_X_MSMETAFILE = "application/x-msmetafile";
  public static final String APPLICATION_X_MSMONEY = "application/x-msmoney";
  public static final String APPLICATION_X_MSPUBLISHER = "application/x-mspublisher";
  public static final String APPLICATION_X_MSSCHEDULE = "application/x-msschedule";
  public static final String APPLICATION_X_MSTERMINAL = "application/x-msterminal";
  public static final String APPLICATION_X_MSWRITE = "application/x-mswrite";
  public static final String APPLICATION_X_NETCDF = "application/x-netcdf";
  public static final String APPLICATION_X_NZB = "application/x-nzb";
  public static final String APPLICATION_X_PKCS12 = "application/x-pkcs12";
  public static final String APPLICATION_X_PKCS7_CERTIFICATES = "application/x-pkcs7-certificates";
  public static final String APPLICATION_X_PKCS7_CERTREQRESP = "application/x-pkcs7-certreqresp";
  public static final String APPLICATION_X_RAR_COMPRESSED = "application/x-rar-compressed";
  public static final String APPLICATION_X_RESEARCH_INFO_SYSTEMS =
      "application/x-research-info-systems";
  public static final String APPLICATION_X_SH = "application/x-sh";
  public static final String APPLICATION_X_SHAR = "application/x-shar";
  public static final String APPLICATION_X_SHOCKWAVE_FLASH = "application/x-shockwave-flash";
  public static final String APPLICATION_X_SILVERLIGHT_APP = "application/x-silverlight-app";
  public static final String APPLICATION_X_SQL = "application/x-sql";
  public static final String APPLICATION_X_STUFFIT = "application/x-stuffit";
  public static final String APPLICATION_X_STUFFITX = "application/x-stuffitx";
  public static final String APPLICATION_X_SUBRIP = "application/x-subrip";
  public static final String APPLICATION_X_SV4CPIO = "application/x-sv4cpio";
  public static final String APPLICATION_X_SV4CRC = "application/x-sv4crc";
  public static final String APPLICATION_X_T3VM_IMAGE = "application/x-t3vm-image";
  public static final String APPLICATION_X_TADS = "application/x-tads";
  public static final String APPLICATION_X_TAR = "application/x-tar";
  public static final String APPLICATION_X_TCL = "application/x-tcl";
  public static final String APPLICATION_X_TEX = "application/x-tex";
  public static final String APPLICATION_X_TEX_TFM = "application/x-tex-tfm";
  public static final String APPLICATION_X_TEXINFO = "application/x-texinfo";
  public static final String APPLICATION_X_TGIF = "application/x-tgif";
  public static final String APPLICATION_X_USTAR = "application/x-ustar";
  public static final String APPLICATION_X_WAIS_SOURCE = "application/x-wais-source";
  public static final String APPLICATION_X_WWW_FORM_URLENCODED =
      "application/x-www-form-urlencoded";
  public static final String APPLICATION_X_X509_CA_CERT = "application/x-x509-ca-cert";
  public static final String APPLICATION_X_XFIG = "application/x-xfig";
  public static final String APPLICATION_X_XLIFF_XML = "application/x-xliff+xml";
  public static final String APPLICATION_X_XPINSTALL = "application/x-xpinstall";
  public static final String APPLICATION_X_XZ = "application/x-xz";
  public static final String APPLICATION_X_ZMACHINE = "application/x-zmachine";
  public static final String APPLICATION_XAML_XML = "application/xaml+xml";
  public static final String APPLICATION_XCAP_DIFF_XML = "application/xcap-diff+xml";
  public static final String APPLICATION_XENC_XML = "application/xenc+xml";
  public static final String APPLICATION_XHTML_XML = "application/xhtml+xml";
  public static final String APPLICATION_XML = "application/xml";
  public static final String APPLICATION_XML_DTD = "application/xml-dtd";
  public static final String APPLICATION_XOP_XML = "application/xop+xml";
  public static final String APPLICATION_XPROC_XML = "application/xproc+xml";
  public static final String APPLICATION_XSLT_XML = "application/xslt+xml";
  public static final String APPLICATION_XSPF_XML = "application/xspf+xml";
  public static final String APPLICATION_XV_XML = "application/xv+xml";
  public static final String APPLICATION_YANG = "application/yang";
  public static final String APPLICATION_YIN_XML = "application/yin+xml";
  public static final String APPLICATION_ZIP = "application/zip";
  public static final String AUDIO_ADPCM = "audio/adpcm";
  public static final String AUDIO_BASIC = "audio/basic";
  public static final String AUDIO_MIDI = "audio/midi";
  public static final String AUDIO_MP4 = "audio/mp4";
  public static final String AUDIO_MPEG = "audio/mpeg";
  public static final String AUDIO_OGG = "audio/ogg";
  public static final String AUDIO_S3M = "audio/s3m";
  public static final String AUDIO_SILK = "audio/silk";
  public static final String AUDIO_VND_DECE_AUDIO = "audio/vnd.dece.audio";
  public static final String AUDIO_VND_DIGITAL_WINDS = "audio/vnd.digital-winds";
  public static final String AUDIO_VND_DRA = "audio/vnd.dra";
  public static final String AUDIO_VND_DTS = "audio/vnd.dts";
  public static final String AUDIO_VND_DTS_HD = "audio/vnd.dts.hd";
  public static final String AUDIO_VND_LUCENT_VOICE = "audio/vnd.lucent.voice";
  public static final String AUDIO_VND_MS_PLAYREADY_MEDIA_PYA = "audio/vnd.ms-playready.media.pya";
  public static final String AUDIO_VND_NUERA_ECELP4800 = "audio/vnd.nuera.ecelp4800";
  public static final String AUDIO_VND_NUERA_ECELP7470 = "audio/vnd.nuera.ecelp7470";
  public static final String AUDIO_VND_NUERA_ECELP9600 = "audio/vnd.nuera.ecelp9600";
  public static final String AUDIO_VND_RIP = "audio/vnd.rip";
  public static final String AUDIO_WEBM = "audio/webm";
  public static final String AUDIO_X_AAC = "audio/x-aac";
  public static final String AUDIO_X_AIFF = "audio/x-aiff";
  public static final String AUDIO_X_CAF = "audio/x-caf";
  public static final String AUDIO_X_FLAC = "audio/x-flac";
  public static final String AUDIO_X_MATROSKA = "audio/x-matroska";
  public static final String AUDIO_X_MPEGURL = "audio/x-mpegurl";
  public static final String AUDIO_X_MS_WAX = "audio/x-ms-wax";
  public static final String AUDIO_X_MS_WMA = "audio/x-ms-wma";
  public static final String AUDIO_X_PN_REALAUDIO = "audio/x-pn-realaudio";
  public static final String AUDIO_X_PN_REALAUDIO_PLUGIN = "audio/x-pn-realaudio-plugin";
  public static final String AUDIO_X_WAV = "audio/x-wav";
  public static final String AUDIO_XM = "audio/xm";
  public static final String CHEMICAL_X_CDX = "chemical/x-cdx";
  public static final String CHEMICAL_X_CIF = "chemical/x-cif";
  public static final String CHEMICAL_X_CMDF = "chemical/x-cmdf";
  public static final String CHEMICAL_X_CML = "chemical/x-cml";
  public static final String CHEMICAL_X_CSML = "chemical/x-csml";
  public static final String CHEMICAL_X_XYZ = "chemical/x-xyz";
  public static final String IMAGE_BMP = "image/bmp";
  public static final String IMAGE_CGM = "image/cgm";
  public static final String IMAGE_G3FAX = "image/g3fax";
  public static final String IMAGE_GIF = "image/gif";
  public static final String IMAGE_IEF = "image/ief";
  public static final String IMAGE_JPEG = "image/jpeg";
  public static final String IMAGE_KTX = "image/ktx";
  public static final String IMAGE_PNG = "image/png";
  public static final String IMAGE_PRS_BTIF = "image/prs.btif";
  public static final String IMAGE_SGI = "image/sgi";
  public static final String IMAGE_SVG_XML = "image/svg+xml";
  public static final String IMAGE_TIFF = "image/tiff";
  public static final String IMAGE_VND_ADOBE_PHOTOSHOP = "image/vnd.adobe.photoshop";
  public static final String IMAGE_VND_DECE_GRAPHIC = "image/vnd.dece.graphic";
  public static final String IMAGE_VND_DVB_SUBTITLE = "image/vnd.dvb.subtitle";
  public static final String IMAGE_VND_DJVU = "image/vnd.djvu";
  public static final String IMAGE_VND_DWG = "image/vnd.dwg";
  public static final String IMAGE_VND_DXF = "image/vnd.dxf";
  public static final String IMAGE_VND_FASTBIDSHEET = "image/vnd.fastbidsheet";
  public static final String IMAGE_VND_FPX = "image/vnd.fpx";
  public static final String IMAGE_VND_FST = "image/vnd.fst";
  public static final String IMAGE_VND_FUJIXEROX_EDMICS_MMR = "image/vnd.fujixerox.edmics-mmr";
  public static final String IMAGE_VND_FUJIXEROX_EDMICS_RLC = "image/vnd.fujixerox.edmics-rlc";
  public static final String IMAGE_VND_MICROSOFT_ICON = "image/vnd.microsoft.icon";
  public static final String IMAGE_VND_MS_MODI = "image/vnd.ms-modi";
  public static final String IMAGE_VND_MS_PHOTO = "image/vnd.ms-photo";
  public static final String IMAGE_VND_NET_FPX = "image/vnd.net-fpx";
  public static final String IMAGE_VND_WAP_WBMP = "image/vnd.wap.wbmp";
  public static final String IMAGE_VND_XIFF = "image/vnd.xiff";
  public static final String IMAGE_WEBP = "image/webp";
  public static final String IMAGE_X_3DS = "image/x-3ds";
  public static final String IMAGE_X_CMU_RASTER = "image/x-cmu-raster";
  public static final String IMAGE_X_CMX = "image/x-cmx";
  public static final String IMAGE_X_FREEHAND = "image/x-freehand";
  public static final String IMAGE_X_ICON = "image/x-icon";
  public static final String IMAGE_X_MRSID_IMAGE = "image/x-mrsid-image";
  public static final String IMAGE_X_PCX = "image/x-pcx";
  public static final String IMAGE_X_PICT = "image/x-pict";
  public static final String IMAGE_X_PORTABLE_ANYMAP = "image/x-portable-anymap";
  public static final String IMAGE_X_PORTABLE_BITMAP = "image/x-portable-bitmap";
  public static final String IMAGE_X_PORTABLE_GRAYMAP = "image/x-portable-graymap";
  public static final String IMAGE_X_PORTABLE_PIXMAP = "image/x-portable-pixmap";
  public static final String IMAGE_X_RGB = "image/x-rgb";
  public static final String IMAGE_X_TGA = "image/x-tga";
  public static final String IMAGE_X_XBITMAP = "image/x-xbitmap";
  public static final String IMAGE_X_XPIXMAP = "image/x-xpixmap";
  public static final String IMAGE_X_XWINDOWDUMP = "image/x-xwindowdump";
  public static final String MESSAGE_RFC822 = "message/rfc822";
  public static final String MODEL_IGES = "model/iges";
  public static final String MODEL_MESH = "model/mesh";
  public static final String MODEL_VND_COLLADA_XML = "model/vnd.collada+xml";
  public static final String MODEL_VND_DWF = "model/vnd.dwf";
  public static final String MODEL_VND_GDL = "model/vnd.gdl";
  public static final String MODEL_VND_GTW = "model/vnd.gtw";
  public static final String MODEL_VND_MTS = "model/vnd.mts";
  public static final String MODEL_VND_VTU = "model/vnd.vtu";
  public static final String MODEL_VRML = "model/vrml";
  public static final String MODEL_X3D_BINARY = "model/x3d+binary";
  public static final String MODEL_X3D_VRML = "model/x3d+vrml";
  public static final String MODEL_X3D_XML = "model/x3d+xml";
  public static final String TEXT_ATLAS_GPS = "text/atlas+gps";
  public static final String TEXT_CACHE_MANIFEST = "text/cache-manifest";
  public static final String TEXT_CALENDAR = "text/calendar";
  public static final String TEXT_CSS = "text/css";
  public static final String TEXT_CSV = "text/csv";
  public static final String TEXT_GPX_XML = "text/gpx+xml";
  public static final String TEXT_HTML = "text/html";
  public static final String TEXT_N3 = "text/n3";
  public static final String TEXT_PLAIN = "text/plain";
  public static final String TEXT_PRS_LINES_TAG = "text/prs.lines.tag";
  public static final String TEXT_RICHTEXT = "text/richtext";
  public static final String TEXT_SGML = "text/sgml";
  public static final String TEXT_TAB_SEPARATED_VALUES = "text/tab-separated-values";
  public static final String TEXT_TROFF = "text/troff";
  public static final String TEXT_TURTLE = "text/turtle";
  public static final String TEXT_URI_LIST = "text/uri-list";
  public static final String TEXT_VCARD = "text/vcard";
  public static final String TEXT_VND_CURL = "text/vnd.curl";
  public static final String TEXT_VND_CURL_DCURL = "text/vnd.curl.dcurl";
  public static final String TEXT_VND_CURL_SCURL = "text/vnd.curl.scurl";
  public static final String TEXT_VND_CURL_MCURL = "text/vnd.curl.mcurl";
  public static final String TEXT_VND_DVB_SUBTITLE = "text/vnd.dvb.subtitle";
  public static final String TEXT_VND_FLY = "text/vnd.fly";
  public static final String TEXT_VND_FMI_FLEXSTOR = "text/vnd.fmi.flexstor";
  public static final String TEXT_VND_GRAPHVIZ = "text/vnd.graphviz";
  public static final String TEXT_VND_IN3D_3DML = "text/vnd.in3d.3dml";
  public static final String TEXT_VND_IN3D_SPOT = "text/vnd.in3d.spot";
  public static final String TEXT_VND_SUN_J2ME_APP_DESCRIPTOR = "text/vnd.sun.j2me.app-descriptor";
  public static final String TEXT_VND_WAP_WML = "text/vnd.wap.wml";
  public static final String TEXT_VND_WAP_WMLSCRIPT = "text/vnd.wap.wmlscript";
  public static final String TEXT_X_ASM = "text/x-asm";
  public static final String TEXT_X_C = "text/x-c";
  public static final String TEXT_X_FORTRAN = "text/x-fortran";
  public static final String TEXT_X_JAVA_SOURCE = "text/x-java-source";
  public static final String TEXT_X_OPML = "text/x-opml";
  public static final String TEXT_X_PASCAL = "text/x-pascal";
  public static final String TEXT_X_NFO = "text/x-nfo";
  public static final String TEXT_X_SETEXT = "text/x-setext";
  public static final String TEXT_X_SFV = "text/x-sfv";
  public static final String TEXT_X_UUENCODE = "text/x-uuencode";
  public static final String TEXT_X_VCALENDAR = "text/x-vcalendar";
  public static final String TEXT_X_VCARD = "text/x-vcard";
  public static final String TEXT_XML = "text/xml";
  public static final String VIDEO_3GPP = "video/3gpp";
  public static final String VIDEO_3GPP2 = "video/3gpp2";
  public static final String VIDEO_H261 = "video/h261";
  public static final String VIDEO_H263 = "video/h263";
  public static final String VIDEO_H264 = "video/h264";
  public static final String VIDEO_JPEG = "video/jpeg";
  public static final String VIDEO_JPM = "video/jpm";
  public static final String VIDEO_MJ2 = "video/mj2";
  public static final String VIDEO_MP4 = "video/mp4";
  public static final String VIDEO_MPEG = "video/mpeg";
  public static final String VIDEO_OGG = "video/ogg";
  public static final String VIDEO_QUICKTIME = "video/quicktime";
  public static final String VIDEO_VND_DECE_HD = "video/vnd.dece.hd";
  public static final String VIDEO_VND_DECE_MOBILE = "video/vnd.dece.mobile";
  public static final String VIDEO_VND_DECE_PD = "video/vnd.dece.pd";
  public static final String VIDEO_VND_DECE_SD = "video/vnd.dece.sd";
  public static final String VIDEO_VND_DECE_VIDEO = "video/vnd.dece.video";
  public static final String VIDEO_VND_DVB_FILE = "video/vnd.dvb.file";
  public static final String VIDEO_VND_FVT = "video/vnd.fvt";
  public static final String VIDEO_VND_MPEGURL = "video/vnd.mpegurl";
  public static final String VIDEO_VND_MS_PLAYREADY_MEDIA_PYV = "video/vnd.ms-playready.media.pyv";
  public static final String VIDEO_VND_UVVU_MP4 = "video/vnd.uvvu.mp4";
  public static final String VIDEO_VND_VIVO = "video/vnd.vivo";
  public static final String VIDEO_WEBM = "video/webm";
  public static final String VIDEO_X_F4V = "video/x-f4v";
  public static final String VIDEO_X_FLI = "video/x-fli";
  public static final String VIDEO_X_FLV = "video/x-flv";
  public static final String VIDEO_X_M4V = "video/x-m4v";
  public static final String VIDEO_X_MATROSKA = "video/x-matroska";
  public static final String VIDEO_X_MNG = "video/x-mng";
  public static final String VIDEO_X_MS_ASF = "video/x-ms-asf";
  public static final String VIDEO_X_MS_VOB = "video/x-ms-vob";
  public static final String VIDEO_X_MS_WM = "video/x-ms-wm";
  public static final String VIDEO_X_MS_WMV = "video/x-ms-wmv";
  public static final String VIDEO_X_MS_WMX = "video/x-ms-wmx";
  public static final String VIDEO_X_MS_WVX = "video/x-ms-wvx";
  public static final String VIDEO_X_MSVIDEO = "video/x-msvideo";
  public static final String VIDEO_X_SGI_MOVIE = "video/x-sgi-movie";
  public static final String VIDEO_X_SMV = "video/x-smv";
  public static final String X_CONFERENCE_X_COOLTALK = "x-conference/x-cooltalk";
  public static final String TEXT_JSON = "text/json";

  static {
    json(TEXT_JSON);
  }

  public static HashMap<String, String> MIMEtoExt =
      new HashMap<>() {

        private static final long serialVersionUID = 8981662833779733468L;

        {
          put(IMAGE_JPEG, "jpg");
          put(IMAGE_PNG, "png");
          put(TEXT_CSV, "csv");
          put(TEXT_PLAIN, "txt");
          put(TEXT_GPX_XML, "gpx");
          put(APPLICATION_GPX_XML, "gpx");
          put(APPLICATION_GEO_JSON, "application/geo+json");
          put(APPLICATION_VND_GOOGLE_EARTH_KML_XML, "kml");
          put(APPLICATION_VND_GOOGLE_EARTH_KMZ, "kmz");
          put(APPLICATION_PDF, "pdf");
          put(APPLICATION_JAVASCRIPT, "js");
          put(APPLICATION_MSWORD, "doc");
          put(APPLICATION_VND_MS_EXCEL, "xls");
          put(APPLICATION_VND_MS_POWERPOINT, "ppt");
          put(APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_WORDPROCESSINGML_DOCUMENT, "docx");
          put(APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_SPREADSHEETML_SHEET, "xlsx");
          put(APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_PRESENTATION, "pptx");
          put(APPLICATION_OCTET_STREAM, "bin");
        }
      };

  public static HashMap<String, String> extToMIME =
      new HashMap<>() {

        private static final long serialVersionUID = 6012810338752724289L;

        {
          put("ez", APPLICATION_ANDREW_INSET);
          put("aw", APPLICATION_APPLIXWARE);
          put("atom", APPLICATION_ATOM_XML);
          put("atomcat", APPLICATION_ATOMCAT_XML);
          put("atomsvc", APPLICATION_ATOMSVC_XML);
          put("ccxml", APPLICATION_CCXML_XML);
          put("cdmia", APPLICATION_CDMI_CAPABILITY);
          put("cdmic", APPLICATION_CDMI_CONTAINER);
          put("cdmid", APPLICATION_CDMI_DOMAIN);
          put("cdmio", APPLICATION_CDMI_OBJECT);
          put("cdmiq", APPLICATION_CDMI_QUEUE);
          put("cu", APPLICATION_CU_SEEME);
          put("davmount", APPLICATION_DAVMOUNT_XML);
          put("dbk", APPLICATION_DOCBOOK_XML);
          put("dssc", APPLICATION_DSSC_DER);
          put("xdssc", APPLICATION_DSSC_XML);
          put("ecma", APPLICATION_ECMASCRIPT);
          put("emma", APPLICATION_EMMA_XML);
          put("epub", APPLICATION_EPUB_ZIP);
          put("exi", APPLICATION_EXI);
          put("pfr", APPLICATION_FONT_TDPFR);
          put("gml", APPLICATION_GML_XML);
          put("gpx", APPLICATION_GPX_XML);
          put("gxf", APPLICATION_GXF);
          put("stk", APPLICATION_HYPERSTUDIO);
          put("ink", APPLICATION_INKML_XML);
          put("inkml", APPLICATION_INKML_XML);
          put("ipfix", APPLICATION_IPFIX);
          put("jar", APPLICATION_JAVA_ARCHIVE);
          put("ser", APPLICATION_JAVA_SERIALIZED_OBJECT);
          put("class", APPLICATION_JAVA_VM);
          put("js", APPLICATION_JAVASCRIPT);
          put("json", APPLICATION_JSON);
          put("geojson", APPLICATION_GEO_JSON);
          put("mvt", APPLICATION_VND_MAPBOX_VECTOR_TILE);
          put("jsonml", APPLICATION_JSONML_JSON);
          put("lostxml", APPLICATION_LOST_XML);
          put("hqx", APPLICATION_MAC_BINHEX40);
          put("cpt", APPLICATION_MAC_COMPACTPRO);
          put("mads", APPLICATION_MADS_XML);
          put("mrc", APPLICATION_MARC);
          put("mrcx", APPLICATION_MARCXML_XML);
          put("ma", APPLICATION_MATHEMATICA);
          put("nb", APPLICATION_MATHEMATICA);
          put("mb", APPLICATION_MATHEMATICA);
          put("mathml", APPLICATION_MATHML_XML);
          put("mbox", APPLICATION_MBOX);
          put("mscml", APPLICATION_MEDIASERVERCONTROL_XML);
          put("metalink", APPLICATION_METALINK_XML);
          put("meta4", APPLICATION_METALINK4_XML);
          put("mets", APPLICATION_METS_XML);
          put("mods", APPLICATION_MODS_XML);
          put("m21", APPLICATION_MP21);
          put("mp21", APPLICATION_MP21);
          put("mp4s", APPLICATION_MP4);
          put("doc", APPLICATION_MSWORD);
          put("dot", APPLICATION_MSWORD);
          put("mxf", APPLICATION_MXF);
          put("bin", APPLICATION_OCTET_STREAM);
          put("dms", APPLICATION_OCTET_STREAM);
          put("lrf", APPLICATION_OCTET_STREAM);
          put("mar", APPLICATION_OCTET_STREAM);
          put("so", APPLICATION_OCTET_STREAM);
          put("dist", APPLICATION_OCTET_STREAM);
          put("distz", APPLICATION_OCTET_STREAM);
          put("pkg", APPLICATION_OCTET_STREAM);
          put("bpk", APPLICATION_OCTET_STREAM);
          put("dump", APPLICATION_OCTET_STREAM);
          put("elc", APPLICATION_OCTET_STREAM);
          put("deploy", APPLICATION_OCTET_STREAM);
          put("oda", APPLICATION_ODA);
          put("opf", APPLICATION_OEBPS_PACKAGE_XML);
          put("ogx", APPLICATION_OGG);
          put("omdoc", APPLICATION_OMDOC_XML);
          put("onetoc", APPLICATION_ONENOTE);
          put("onetoc2", APPLICATION_ONENOTE);
          put("onetmp", APPLICATION_ONENOTE);
          put("onepkg", APPLICATION_ONENOTE);
          put("oxps", APPLICATION_OXPS);
          put("xer", APPLICATION_PATCH_OPS_ERROR_XML);
          put("pdf", APPLICATION_PDF);
          put("pgp", APPLICATION_PGP_ENCRYPTED);
          put("asc", APPLICATION_PGP_SIGNATURE);
          put("sig", APPLICATION_PGP_SIGNATURE);
          put("prf", APPLICATION_PICS_RULES);
          put("p10", APPLICATION_PKCS10);
          put("p7m", APPLICATION_PKCS7_MIME);
          put("p7c", APPLICATION_PKCS7_MIME);
          put("p7s", APPLICATION_PKCS7_SIGNATURE);
          put("p8", APPLICATION_PKCS8);
          put("ac", APPLICATION_PKIX_ATTR_CERT);
          put("cer", APPLICATION_PKIX_CERT);
          put("crl", APPLICATION_PKIX_CRL);
          put("pkipath", APPLICATION_PKIX_PKIPATH);
          put("pki", APPLICATION_PKIXCMP);
          put("pls", APPLICATION_PLS_XML);
          put("ai", APPLICATION_POSTSCRIPT);
          put("eps", APPLICATION_POSTSCRIPT);
          put("ps", APPLICATION_POSTSCRIPT);
          put("cww", APPLICATION_PRS_CWW);
          put("pskcxml", APPLICATION_PSKC_XML);
          put("rdf", APPLICATION_RDF_XML);
          put("rif", APPLICATION_REGINFO_XML);
          put("rnc", APPLICATION_RELAX_NG_COMPACT_SYNTAX);
          put("rl", APPLICATION_RESOURCE_LISTS_XML);
          put("rld", APPLICATION_RESOURCE_LISTS_DIFF_XML);
          put("rs", APPLICATION_RLS_SERVICES_XML);
          put("gbr", APPLICATION_RPKI_GHOSTBUSTERS);
          put("mft", APPLICATION_RPKI_MANIFEST);
          put("roa", APPLICATION_RPKI_ROA);
          put("rsd", APPLICATION_RSD_XML);
          put("rss", APPLICATION_RSS_XML);
          put("rtf", APPLICATION_RTF);
          put("sbml", APPLICATION_SBML_XML);
          put("scq", APPLICATION_SCVP_CV_REQUEST);
          put("scs", APPLICATION_SCVP_CV_RESPONSE);
          put("spq", APPLICATION_SCVP_VP_REQUEST);
          put("spp", APPLICATION_SCVP_VP_RESPONSE);
          put("sdp", APPLICATION_SDP);
          put("setpay", APPLICATION_SET_PAYMENT_INITIATION);
          put("setreg", APPLICATION_SET_REGISTRATION_INITIATION);
          put("shf", APPLICATION_SHF_XML);
          put("smi", APPLICATION_SMIL_XML);
          put("smil", APPLICATION_SMIL_XML);
          put("rq", APPLICATION_SPARQL_QUERY);
          put("srx", APPLICATION_SPARQL_RESULTS_XML);
          put("gram", APPLICATION_SRGS);
          put("grxml", APPLICATION_SRGS_XML);
          put("sru", APPLICATION_SRU_XML);
          put("ssdl", APPLICATION_SSDL_XML);
          put("ssml", APPLICATION_SSML_XML);
          put("tei", APPLICATION_TEI_XML);
          put("teicorpus", APPLICATION_TEI_XML);
          put("tfi", APPLICATION_THRAUD_XML);
          put("tsd", APPLICATION_TIMESTAMPED_DATA);
          put("plb", APPLICATION_VND_3GPP_PIC_BW_LARGE);
          put("psb", APPLICATION_VND_3GPP_PIC_BW_SMALL);
          put("pvb", APPLICATION_VND_3GPP_PIC_BW_VAR);
          put("tcap", APPLICATION_VND_3GPP2_TCAP);
          put("pwn", APPLICATION_VND_3M_POST_IT_NOTES);
          put("aso", APPLICATION_VND_ACCPAC_SIMPLY_ASO);
          put("imp", APPLICATION_VND_ACCPAC_SIMPLY_IMP);
          put("acu", APPLICATION_VND_ACUCOBOL);
          put("atc", APPLICATION_VND_ACUCORP);
          put("acutc", APPLICATION_VND_ACUCORP);
          put("air", APPLICATION_VND_ADOBE_AIR_APPLICATION_INSTALLER_PACKAGE_ZIP);
          put("fcdt", APPLICATION_VND_ADOBE_FORMSCENTRAL_FCDT);
          put("fxp", APPLICATION_VND_ADOBE_FXP);
          put("fxpl", APPLICATION_VND_ADOBE_FXP);
          put("xdp", APPLICATION_VND_ADOBE_XDP_XML);
          put("xfdf", APPLICATION_VND_ADOBE_XFDF);
          put("ahead", APPLICATION_VND_AHEAD_SPACE);
          put("azf", APPLICATION_VND_AIRZIP_FILESECURE_AZF);
          put("azs", APPLICATION_VND_AIRZIP_FILESECURE_AZS);
          put("azw", APPLICATION_VND_AMAZON_EBOOK);
          put("acc", APPLICATION_VND_AMERICANDYNAMICS_ACC);
          put("ami", APPLICATION_VND_AMIGA_AMI);
          put("apk", APPLICATION_VND_ANDROID_PACKAGE_ARCHIVE);
          put("cii", APPLICATION_VND_ANSER_WEB_CERTIFICATE_ISSUE_INITIATION);
          put("fti", APPLICATION_VND_ANSER_WEB_FUNDS_TRANSFER_INITIATION);
          put("atx", APPLICATION_VND_ANTIX_GAME_COMPONENT);
          put("mpkg", APPLICATION_VND_APPLE_INSTALLER_XML);
          put("m3u8", APPLICATION_VND_APPLE_MPEGURL);
          put("swi", APPLICATION_VND_ARISTANETWORKS_SWI);
          put("iota", APPLICATION_VND_ASTRAEA_SOFTWARE_IOTA);
          put("aep", APPLICATION_VND_AUDIOGRAPH);
          put("mpm", APPLICATION_VND_BLUEICE_MULTIPASS);
          put("bmi", APPLICATION_VND_BMI);
          put("rep", APPLICATION_VND_BUSINESSOBJECTS);
          put("cdxml", APPLICATION_VND_CHEMDRAW_XML);
          put("mmd", APPLICATION_VND_CHIPNUTS_KARAOKE_MMD);
          put("cdy", APPLICATION_VND_CINDERELLA);
          put("cla", APPLICATION_VND_CLAYMORE);
          put("rp9", APPLICATION_VND_CLOANTO_RP9);
          put("c4g", APPLICATION_VND_CLONK_C4GROUP);
          put("c4d", APPLICATION_VND_CLONK_C4GROUP);
          put("c4f", APPLICATION_VND_CLONK_C4GROUP);
          put("c4p", APPLICATION_VND_CLONK_C4GROUP);
          put("c4u", APPLICATION_VND_CLONK_C4GROUP);
          put("c11amc", APPLICATION_VND_CLUETRUST_CARTOMOBILE_CONFIG);
          put("c11amz", APPLICATION_VND_CLUETRUST_CARTOMOBILE_CONFIG_PKG);
          put("csp", APPLICATION_VND_COMMONSPACE);
          put("cdbcmsg", APPLICATION_VND_CONTACT_CMSG);
          put("cmc", APPLICATION_VND_COSMOCALLER);
          put("clkx", APPLICATION_VND_CRICK_CLICKER);
          put("clkk", APPLICATION_VND_CRICK_CLICKER_KEYBOARD);
          put("clkp", APPLICATION_VND_CRICK_CLICKER_PALETTE);
          put("clkt", APPLICATION_VND_CRICK_CLICKER_TEMPLATE);
          put("clkw", APPLICATION_VND_CRICK_CLICKER_WORDBANK);
          put("wbs", APPLICATION_VND_CRITICALTOOLS_WBS_XML);
          put("pml", APPLICATION_VND_CTC_POSML);
          put("ppd", APPLICATION_VND_CUPS_PPD);
          put("car", APPLICATION_VND_CURL_CAR);
          put("pcurl", APPLICATION_VND_CURL_PCURL);
          put("dart", APPLICATION_VND_DART);
          put("rdz", APPLICATION_VND_DATA_VISION_RDZ);
          put("uvf", APPLICATION_VND_DECE_DATA);
          put("uvvf", APPLICATION_VND_DECE_DATA);
          put("uvd", APPLICATION_VND_DECE_DATA);
          put("uvvd", APPLICATION_VND_DECE_DATA);
          put("uvt", APPLICATION_VND_DECE_TTML_XML);
          put("uvvt", APPLICATION_VND_DECE_TTML_XML);
          put("uvx", APPLICATION_VND_DECE_UNSPECIFIED);
          put("uvvx", APPLICATION_VND_DECE_UNSPECIFIED);
          put("uvz", APPLICATION_VND_DECE_ZIP);
          put("uvvz", APPLICATION_VND_DECE_ZIP);
          put("fe_launch", APPLICATION_VND_DENOVO_FCSELAYOUT_LINK);
          put("dna", APPLICATION_VND_DNA);
          put("mlp", APPLICATION_VND_DOLBY_MLP);
          put("dpg", APPLICATION_VND_DPGRAPH);
          put("dfac", APPLICATION_VND_DREAMFACTORY);
          put("kpxx", APPLICATION_VND_DS_KEYPOINT);
          put("ait", APPLICATION_VND_DVB_AIT);
          put("svc", APPLICATION_VND_DVB_SERVICE);
          put("geo", APPLICATION_VND_DYNAGEO);
          put("mag", APPLICATION_VND_ECOWIN_CHART);
          put("nml", APPLICATION_VND_ENLIVEN);
          put("esf", APPLICATION_VND_EPSON_ESF);
          put("msf", APPLICATION_VND_EPSON_MSF);
          put("qam", APPLICATION_VND_EPSON_QUICKANIME);
          put("slt", APPLICATION_VND_EPSON_SALT);
          put("ssf", APPLICATION_VND_EPSON_SSF);
          put("es3", APPLICATION_VND_ESZIGNO3_XML);
          put("et3", APPLICATION_VND_ESZIGNO3_XML);
          put("ez2", APPLICATION_VND_EZPIX_ALBUM);
          put("ez3", APPLICATION_VND_EZPIX_PACKAGE);
          put("fdf", APPLICATION_VND_FDF);
          put("mseed", APPLICATION_VND_FDSN_MSEED);
          put("seed", APPLICATION_VND_FDSN_SEED);
          put("dataless", APPLICATION_VND_FDSN_SEED);
          put("gph", APPLICATION_VND_FLOGRAPHIT);
          put("ftc", APPLICATION_VND_FLUXTIME_CLIP);
          put("fm", APPLICATION_VND_FRAMEMAKER);
          put("frame", APPLICATION_VND_FRAMEMAKER);
          put("maker", APPLICATION_VND_FRAMEMAKER);
          put("book", APPLICATION_VND_FRAMEMAKER);
          put("fnc", APPLICATION_VND_FROGANS_FNC);
          put("ltf", APPLICATION_VND_FROGANS_LTF);
          put("fsc", APPLICATION_VND_FSC_WEBLAUNCH);
          put("oas", APPLICATION_VND_FUJITSU_OASYS);
          put("oa2", APPLICATION_VND_FUJITSU_OASYS2);
          put("oa3", APPLICATION_VND_FUJITSU_OASYS3);
          put("fg5", APPLICATION_VND_FUJITSU_OASYSGP);
          put("bh2", APPLICATION_VND_FUJITSU_OASYSPRS);
          put("ddd", APPLICATION_VND_FUJIXEROX_DDD);
          put("xdw", APPLICATION_VND_FUJIXEROX_DOCUWORKS);
          put("xbd", APPLICATION_VND_FUJIXEROX_DOCUWORKS_BINDER);
          put("fzs", APPLICATION_VND_FUZZYSHEET);
          put("txd", APPLICATION_VND_GENOMATIX_TUXEDO);
          put("ggb", APPLICATION_VND_GEOGEBRA_FILE);
          put("ggt", APPLICATION_VND_GEOGEBRA_TOOL);
          put("gex", APPLICATION_VND_GEOMETRY_EXPLORER);
          put("gre", APPLICATION_VND_GEOMETRY_EXPLORER);
          put("gxt", APPLICATION_VND_GEONEXT);
          put("g2w", APPLICATION_VND_GEOPLAN);
          put("g3w", APPLICATION_VND_GEOSPACE);
          put("gmx", APPLICATION_VND_GMX);
          put("kml", APPLICATION_VND_GOOGLE_EARTH_KML_XML);
          put("kmz", APPLICATION_VND_GOOGLE_EARTH_KMZ);
          put("gqf", APPLICATION_VND_GRAFEQ);
          put("gqs", APPLICATION_VND_GRAFEQ);
          put("gac", APPLICATION_VND_GROOVE_ACCOUNT);
          put("ghf", APPLICATION_VND_GROOVE_HELP);
          put("gim", APPLICATION_VND_GROOVE_IDENTITY_MESSAGE);
          put("grv", APPLICATION_VND_GROOVE_INJECTOR);
          put("gtm", APPLICATION_VND_GROOVE_TOOL_MESSAGE);
          put("tpl", APPLICATION_VND_GROOVE_TOOL_TEMPLATE);
          put("vcg", APPLICATION_VND_GROOVE_VCARD);
          put("hal", APPLICATION_VND_HAL_XML);
          put("zmm", APPLICATION_VND_HANDHELD_ENTERTAINMENT_XML);
          put("hbci", APPLICATION_VND_HBCI);
          put("les", APPLICATION_VND_HHE_LESSON_PLAYER);
          put("hpgl", APPLICATION_VND_HP_HPGL);
          put("hpid", APPLICATION_VND_HP_HPID);
          put("hps", APPLICATION_VND_HP_HPS);
          put("jlt", APPLICATION_VND_HP_JLYT);
          put("pcl", APPLICATION_VND_HP_PCL);
          put("pclxl", APPLICATION_VND_HP_PCLXL);
          put("sfd-hdstx", APPLICATION_VND_HYDROSTATIX_SOF_DATA);
          put("mpy", APPLICATION_VND_IBM_MINIPAY);
          put("afp", APPLICATION_VND_IBM_MODCAP);
          put("listafp", APPLICATION_VND_IBM_MODCAP);
          put("list3820", APPLICATION_VND_IBM_MODCAP);
          put("irm", APPLICATION_VND_IBM_RIGHTS_MANAGEMENT);
          put("sc", APPLICATION_VND_IBM_SECURE_CONTAINER);
          put("icc", APPLICATION_VND_ICCPROFILE);
          put("icm", APPLICATION_VND_ICCPROFILE);
          put("igl", APPLICATION_VND_IGLOADER);
          put("ivp", APPLICATION_VND_IMMERVISION_IVP);
          put("ivu", APPLICATION_VND_IMMERVISION_IVU);
          put("igm", APPLICATION_VND_INSORS_IGM);
          put("xpw", APPLICATION_VND_INTERCON_FORMNET);
          put("xpx", APPLICATION_VND_INTERCON_FORMNET);
          put("i2g", APPLICATION_VND_INTERGEO);
          put("qbo", APPLICATION_VND_INTU_QBO);
          put("qfx", APPLICATION_VND_INTU_QFX);
          put("rcprofile", APPLICATION_VND_IPUNPLUGGED_RCPROFILE);
          put("irp", APPLICATION_VND_IREPOSITORY_PACKAGE_XML);
          put("xpr", APPLICATION_VND_IS_XPR);
          put("fcs", APPLICATION_VND_ISAC_FCS);
          put("jam", APPLICATION_VND_JAM);
          put("rms", APPLICATION_VND_JCP_JAVAME_MIDLET_RMS);
          put("jisp", APPLICATION_VND_JISP);
          put("joda", APPLICATION_VND_JOOST_JODA_ARCHIVE);
          put("ktz", APPLICATION_VND_KAHOOTZ);
          put("ktr", APPLICATION_VND_KAHOOTZ);
          put("karbon", APPLICATION_VND_KDE_KARBON);
          put("chrt", APPLICATION_VND_KDE_KCHART);
          put("kfo", APPLICATION_VND_KDE_KFORMULA);
          put("flw", APPLICATION_VND_KDE_KIVIO);
          put("kon", APPLICATION_VND_KDE_KONTOUR);
          put("kpr", APPLICATION_VND_KDE_KPRESENTER);
          put("kpt", APPLICATION_VND_KDE_KPRESENTER);
          put("ksp", APPLICATION_VND_KDE_KSPREAD);
          put("kwd", APPLICATION_VND_KDE_KWORD);
          put("kwt", APPLICATION_VND_KDE_KWORD);
          put("htke", APPLICATION_VND_KENAMEAAPP);
          put("kia", APPLICATION_VND_KIDSPIRATION);
          put("kne", APPLICATION_VND_KINAR);
          put("knp", APPLICATION_VND_KINAR);
          put("skp", APPLICATION_VND_KOAN);
          put("skd", APPLICATION_VND_KOAN);
          put("skt", APPLICATION_VND_KOAN);
          put("skm", APPLICATION_VND_KOAN);
          put("sse", APPLICATION_VND_KODAK_DESCRIPTOR);
          put("lasxml", APPLICATION_VND_LAS_LAS_XML);
          put("lbd", APPLICATION_VND_LLAMAGRAPHICS_LIFE_BALANCE_DESKTOP);
          put("lbe", APPLICATION_VND_LLAMAGRAPHICS_LIFE_BALANCE_EXCHANGE_XML);
          put("123", APPLICATION_VND_LOTUS_1_2_3);
          put("apr", APPLICATION_VND_LOTUS_APPROACH);
          put("pre", APPLICATION_VND_LOTUS_FREELANCE);
          put("nsf", APPLICATION_VND_LOTUS_NOTES);
          put("org", APPLICATION_VND_LOTUS_ORGANIZER);
          put("scm", APPLICATION_VND_LOTUS_SCREENCAM);
          put("lwp", APPLICATION_VND_LOTUS_WORDPRO);
          put("portpkg", APPLICATION_VND_MACPORTS_PORTPKG);
          put("mcd", APPLICATION_VND_MCD);
          put("mc1", APPLICATION_VND_MEDCALCDATA);
          put("cdkey", APPLICATION_VND_MEDIASTATION_CDKEY);
          put("mwf", APPLICATION_VND_MFER);
          put("mfm", APPLICATION_VND_MFMP);
          put("flo", APPLICATION_VND_MICROGRAFX_FLO);
          put("igx", APPLICATION_VND_MICROGRAFX_IGX);
          put("mif", APPLICATION_VND_MIF);
          put("daf", APPLICATION_VND_MOBIUS_DAF);
          put("dis", APPLICATION_VND_MOBIUS_DIS);
          put("mbk", APPLICATION_VND_MOBIUS_MBK);
          put("mqy", APPLICATION_VND_MOBIUS_MQY);
          put("msl", APPLICATION_VND_MOBIUS_MSL);
          put("plc", APPLICATION_VND_MOBIUS_PLC);
          put("txf", APPLICATION_VND_MOBIUS_TXF);
          put("mpn", APPLICATION_VND_MOPHUN_APPLICATION);
          put("mpc", APPLICATION_VND_MOPHUN_CERTIFICATE);
          put("xul", APPLICATION_VND_MOZILLA_XUL_XML);
          put("cil", APPLICATION_VND_MS_ARTGALRY);
          put("cab", APPLICATION_VND_MS_CAB_COMPRESSED);
          put("xls", APPLICATION_VND_MS_EXCEL);
          put("xlm", APPLICATION_VND_MS_EXCEL);
          put("xla", APPLICATION_VND_MS_EXCEL);
          put("xlc", APPLICATION_VND_MS_EXCEL);
          put("xlt", APPLICATION_VND_MS_EXCEL);
          put("xlw", APPLICATION_VND_MS_EXCEL);
          put("xlam", APPLICATION_VND_MS_EXCEL_ADDIN_MACROENABLED_12);
          put("xlsb", APPLICATION_VND_MS_EXCEL_SHEET_BINARY_MACROENABLED_12);
          put("xlsm", APPLICATION_VND_MS_EXCEL_SHEET_MACROENABLED_12);
          put("xltm", APPLICATION_VND_MS_EXCEL_TEMPLATE_MACROENABLED_12);
          put("eot", APPLICATION_VND_MS_FONTOBJECT);
          put("chm", APPLICATION_VND_MS_HTMLHELP);
          put("ims", APPLICATION_VND_MS_IMS);
          put("lrm", APPLICATION_VND_MS_LRM);
          put("thmx", APPLICATION_VND_MS_OFFICETHEME);
          put("cat", APPLICATION_VND_MS_PKI_SECCAT);
          put("stl", APPLICATION_VND_MS_PKI_STL);
          put("ppt", APPLICATION_VND_MS_POWERPOINT);
          put("pps", APPLICATION_VND_MS_POWERPOINT);
          put("pot", APPLICATION_VND_MS_POWERPOINT);
          put("ppam", APPLICATION_VND_MS_POWERPOINT_ADDIN_MACROENABLED_12);
          put("pptm", APPLICATION_VND_MS_POWERPOINT_PRESENTATION_MACROENABLED_12);
          put("sldm", APPLICATION_VND_MS_POWERPOINT_SLIDE_MACROENABLED_12);
          put("ppsm", APPLICATION_VND_MS_POWERPOINT_SLIDESHOW_MACROENABLED_12);
          put("potm", APPLICATION_VND_MS_POWERPOINT_TEMPLATE_MACROENABLED_12);
          put("mpp", APPLICATION_VND_MS_PROJECT);
          put("mpt", APPLICATION_VND_MS_PROJECT);
          put("docm", APPLICATION_VND_MS_WORD_DOCUMENT_MACROENABLED_12);
          put("dotm", APPLICATION_VND_MS_WORD_TEMPLATE_MACROENABLED_12);
          put("wps", APPLICATION_VND_MS_WORKS);
          put("wks", APPLICATION_VND_MS_WORKS);
          put("wcm", APPLICATION_VND_MS_WORKS);
          put("wdb", APPLICATION_VND_MS_WORKS);
          put("wpl", APPLICATION_VND_MS_WPL);
          put("xps", APPLICATION_VND_MS_XPSDOCUMENT);
          put("mseq", APPLICATION_VND_MSEQ);
          put("mus", APPLICATION_VND_MUSICIAN);
          put("msty", APPLICATION_VND_MUVEE_STYLE);
          put("taglet", APPLICATION_VND_MYNFC);
          put("nlu", APPLICATION_VND_NEUROLANGUAGE_NLU);
          put("ntf", APPLICATION_VND_NITF);
          put("nitf", APPLICATION_VND_NITF);
          put("nnd", APPLICATION_VND_NOBLENET_DIRECTORY);
          put("nns", APPLICATION_VND_NOBLENET_SEALER);
          put("nnw", APPLICATION_VND_NOBLENET_WEB);
          put("ngdat", APPLICATION_VND_NOKIA_N_GAGE_DATA);
          put("n-gage", APPLICATION_VND_NOKIA_N_GAGE_SYMBIAN_INSTALL);
          put("rpst", APPLICATION_VND_NOKIA_RADIO_PRESET);
          put("rpss", APPLICATION_VND_NOKIA_RADIO_PRESETS);
          put("edm", APPLICATION_VND_NOVADIGM_EDM);
          put("edx", APPLICATION_VND_NOVADIGM_EDX);
          put("ext", APPLICATION_VND_NOVADIGM_EXT);
          put("odc", APPLICATION_VND_OASIS_OPENDOCUMENT_CHART);
          put("otc", APPLICATION_VND_OASIS_OPENDOCUMENT_CHART_TEMPLATE);
          put("odb", APPLICATION_VND_OASIS_OPENDOCUMENT_DATABASE);
          put("odf", APPLICATION_VND_OASIS_OPENDOCUMENT_FORMULA);
          put("odft", APPLICATION_VND_OASIS_OPENDOCUMENT_FORMULA_TEMPLATE);
          put("odg", APPLICATION_VND_OASIS_OPENDOCUMENT_GRAPHICS);
          put("otg", APPLICATION_VND_OASIS_OPENDOCUMENT_GRAPHICS_TEMPLATE);
          put("odi", APPLICATION_VND_OASIS_OPENDOCUMENT_IMAGE);
          put("oti", APPLICATION_VND_OASIS_OPENDOCUMENT_IMAGE_TEMPLATE);
          put("odp", APPLICATION_VND_OASIS_OPENDOCUMENT_PRESENTATION);
          put("otp", APPLICATION_VND_OASIS_OPENDOCUMENT_PRESENTATION_TEMPLATE);
          put("ods", APPLICATION_VND_OASIS_OPENDOCUMENT_SPREADSHEET);
          put("ots", APPLICATION_VND_OASIS_OPENDOCUMENT_SPREADSHEET_TEMPLATE);
          put("odt", APPLICATION_VND_OASIS_OPENDOCUMENT_TEXT);
          put("odm", APPLICATION_VND_OASIS_OPENDOCUMENT_TEXT_MASTER);
          put("ott", APPLICATION_VND_OASIS_OPENDOCUMENT_TEXT_TEMPLATE);
          put("oth", APPLICATION_VND_OASIS_OPENDOCUMENT_TEXT_WEB);
          put("xo", APPLICATION_VND_OLPC_SUGAR);
          put("dd2", APPLICATION_VND_OMA_DD2_XML);
          put("oxt", APPLICATION_VND_OPENOFFICEORG_EXTENSION);
          put("pptx", APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_PRESENTATION);
          put("sldx", APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_SLIDE);
          put("ppsx", APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_SLIDESHOW);
          put("potx", APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_TEMPLATE);
          put("xlsx", APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_SPREADSHEETML_SHEET);
          put("xltx", APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_SPREADSHEETML_TEMPLATE);
          put("docx", APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_WORDPROCESSINGML_DOCUMENT);
          put("dotx", APPLICATION_VND_OPENXMLFORMATS_OFFICEDOCUMENT_WORDPROCESSINGML_TEMPLATE);
          put("mgp", APPLICATION_VND_OSGEO_MAPGUIDE_PACKAGE);
          put("dp", APPLICATION_VND_OSGI_DP);
          put("esa", APPLICATION_VND_OSGI_SUBSYSTEM);
          put("pdb", APPLICATION_VND_PALM);
          put("pqa", APPLICATION_VND_PALM);
          put("oprc", APPLICATION_VND_PALM);
          put("paw", APPLICATION_VND_PAWAAFILE);
          put("str", APPLICATION_VND_PG_FORMAT);
          put("ei6", APPLICATION_VND_PG_OSASLI);
          put("efif", APPLICATION_VND_PICSEL);
          put("wg", APPLICATION_VND_PMI_WIDGET);
          put("plf", APPLICATION_VND_POCKETLEARN);
          put("pbd", APPLICATION_VND_POWERBUILDER6);
          put("box", APPLICATION_VND_PREVIEWSYSTEMS_BOX);
          put("mgz", APPLICATION_VND_PROTEUS_MAGAZINE);
          put("qps", APPLICATION_VND_PUBLISHARE_DELTA_TREE);
          put("ptid", APPLICATION_VND_PVI_PTID1);
          put("qxd", APPLICATION_VND_QUARK_QUARKXPRESS);
          put("qxt", APPLICATION_VND_QUARK_QUARKXPRESS);
          put("qwd", APPLICATION_VND_QUARK_QUARKXPRESS);
          put("qwt", APPLICATION_VND_QUARK_QUARKXPRESS);
          put("qxl", APPLICATION_VND_QUARK_QUARKXPRESS);
          put("qxb", APPLICATION_VND_QUARK_QUARKXPRESS);
          put("bed", APPLICATION_VND_REALVNC_BED);
          put("mxl", APPLICATION_VND_RECORDARE_MUSICXML);
          put("musicxml", APPLICATION_VND_RECORDARE_MUSICXML_XML);
          put("cryptonote", APPLICATION_VND_RIG_CRYPTONOTE);
          put("cod", APPLICATION_VND_RIM_COD);
          put("rm", APPLICATION_VND_RN_REALMEDIA);
          put("rmvb", APPLICATION_VND_RN_REALMEDIA_VBR);
          put("link66", APPLICATION_VND_ROUTE66_LINK66_XML);
          put("st", APPLICATION_VND_SAILINGTRACKER_TRACK);
          put("see", APPLICATION_VND_SEEMAIL);
          put("sema", APPLICATION_VND_SEMA);
          put("semd", APPLICATION_VND_SEMD);
          put("semf", APPLICATION_VND_SEMF);
          put("ifm", APPLICATION_VND_SHANA_INFORMED_FORMDATA);
          put("itp", APPLICATION_VND_SHANA_INFORMED_FORMTEMPLATE);
          put("iif", APPLICATION_VND_SHANA_INFORMED_INTERCHANGE);
          put("ipk", APPLICATION_VND_SHANA_INFORMED_PACKAGE);
          put("twd", APPLICATION_VND_SIMTECH_MINDMAPPER);
          put("twds", APPLICATION_VND_SIMTECH_MINDMAPPER);
          put("mmf", APPLICATION_VND_SMAF);
          put("teacher", APPLICATION_VND_SMART_TEACHER);
          put("sdkm", APPLICATION_VND_SOLENT_SDKM_XML);
          put("sdkd", APPLICATION_VND_SOLENT_SDKM_XML);
          put("dxp", APPLICATION_VND_SPOTFIRE_DXP);
          put("sfs", APPLICATION_VND_SPOTFIRE_SFS);
          put("sdc", APPLICATION_VND_STARDIVISION_CALC);
          put("sda", APPLICATION_VND_STARDIVISION_DRAW);
          put("sdd", APPLICATION_VND_STARDIVISION_IMPRESS);
          put("smf", APPLICATION_VND_STARDIVISION_MATH);
          put("sdw", APPLICATION_VND_STARDIVISION_WRITER);
          put("vor", APPLICATION_VND_STARDIVISION_WRITER);
          put("sgl", APPLICATION_VND_STARDIVISION_WRITER_GLOBAL);
          put("smzip", APPLICATION_VND_STEPMANIA_PACKAGE);
          put("sm", APPLICATION_VND_STEPMANIA_STEPCHART);
          put("sxc", APPLICATION_VND_SUN_XML_CALC);
          put("stc", APPLICATION_VND_SUN_XML_CALC_TEMPLATE);
          put("sxd", APPLICATION_VND_SUN_XML_DRAW);
          put("std", APPLICATION_VND_SUN_XML_DRAW_TEMPLATE);
          put("sxi", APPLICATION_VND_SUN_XML_IMPRESS);
          put("sti", APPLICATION_VND_SUN_XML_IMPRESS_TEMPLATE);
          put("sxm", APPLICATION_VND_SUN_XML_MATH);
          put("sxw", APPLICATION_VND_SUN_XML_WRITER);
          put("sxg", APPLICATION_VND_SUN_XML_WRITER_GLOBAL);
          put("stw", APPLICATION_VND_SUN_XML_WRITER_TEMPLATE);
          put("sus", APPLICATION_VND_SUS_CALENDAR);
          put("susp", APPLICATION_VND_SUS_CALENDAR);
          put("svd", APPLICATION_VND_SVD);
          put("sis", APPLICATION_VND_SYMBIAN_INSTALL);
          put("sisx", APPLICATION_VND_SYMBIAN_INSTALL);
          put("xsm", APPLICATION_VND_SYNCML_XML);
          put("bdm", APPLICATION_VND_SYNCML_DM_WBXML);
          put("xdm", APPLICATION_VND_SYNCML_DM_XML);
          put("tao", APPLICATION_VND_TAO_INTENT_MODULE_ARCHIVE);
          put("pcap", APPLICATION_VND_TCPDUMP_PCAP);
          put("cap", APPLICATION_VND_TCPDUMP_PCAP);
          put("dmp", APPLICATION_VND_TCPDUMP_PCAP);
          put("tmo", APPLICATION_VND_TMOBILE_LIVETV);
          put("tpt", APPLICATION_VND_TRID_TPT);
          put("mxs", APPLICATION_VND_TRISCAPE_MXS);
          put("tra", APPLICATION_VND_TRUEAPP);
          put("ufd", APPLICATION_VND_UFDL);
          put("ufdl", APPLICATION_VND_UFDL);
          put("utz", APPLICATION_VND_UIQ_THEME);
          put("umj", APPLICATION_VND_UMAJIN);
          put("unityweb", APPLICATION_VND_UNITY);
          put("uoml", APPLICATION_VND_UOML_XML);
          put("vcx", APPLICATION_VND_VCX);
          put("vsd", APPLICATION_VND_VISIO);
          put("vst", APPLICATION_VND_VISIO);
          put("vss", APPLICATION_VND_VISIO);
          put("vsw", APPLICATION_VND_VISIO);
          put("vis", APPLICATION_VND_VISIONARY);
          put("vsf", APPLICATION_VND_VSF);
          put("wbxml", APPLICATION_VND_WAP_WBXML);
          put("wmlc", APPLICATION_VND_WAP_WMLC);
          put("wmlsc", APPLICATION_VND_WAP_WMLSCRIPTC);
          put("wtb", APPLICATION_VND_WEBTURBO);
          put("nbp", APPLICATION_VND_WOLFRAM_PLAYER);
          put("wpd", APPLICATION_VND_WORDPERFECT);
          put("wqd", APPLICATION_VND_WQD);
          put("stf", APPLICATION_VND_WT_STF);
          put("xar", APPLICATION_VND_XARA);
          put("xfdl", APPLICATION_VND_XFDL);
          put("hvd", APPLICATION_VND_YAMAHA_HV_DIC);
          put("hvs", APPLICATION_VND_YAMAHA_HV_SCRIPT);
          put("hvp", APPLICATION_VND_YAMAHA_HV_VOICE);
          put("osf", APPLICATION_VND_YAMAHA_OPENSCOREFORMAT);
          put("osfpvg", APPLICATION_VND_YAMAHA_OPENSCOREFORMAT_OSFPVG_XML);
          put("saf", APPLICATION_VND_YAMAHA_SMAF_AUDIO);
          put("spf", APPLICATION_VND_YAMAHA_SMAF_PHRASE);
          put("cmp", APPLICATION_VND_YELLOWRIVER_CUSTOM_MENU);
          put("zir", APPLICATION_VND_ZUL);
          put("zirz", APPLICATION_VND_ZUL);
          put("zaz", APPLICATION_VND_ZZAZZ_DECK_XML);
          put("vxml", APPLICATION_VOICEXML_XML);
          put("wgt", APPLICATION_WIDGET);
          put("hlp", APPLICATION_WINHLP);
          put("wsdl", APPLICATION_WSDL_XML);
          put("wspolicy", APPLICATION_WSPOLICY_XML);
          put("7z", APPLICATION_X_7Z_COMPRESSED);
          put("abw", APPLICATION_X_ABIWORD);
          put("ace", APPLICATION_X_ACE_COMPRESSED);
          put("dmg", APPLICATION_X_APPLE_DISKIMAGE);
          put("aab", APPLICATION_X_AUTHORWARE_BIN);
          put("x32", APPLICATION_X_AUTHORWARE_BIN);
          put("u32", APPLICATION_X_AUTHORWARE_BIN);
          put("vox", APPLICATION_X_AUTHORWARE_BIN);
          put("aam", APPLICATION_X_AUTHORWARE_MAP);
          put("aas", APPLICATION_X_AUTHORWARE_SEG);
          put("bcpio", APPLICATION_X_BCPIO);
          put("torrent", APPLICATION_X_BITTORRENT);
          put("blb", APPLICATION_X_BLORB);
          put("blorb", APPLICATION_X_BLORB);
          put("bz", APPLICATION_X_BZIP);
          put("bz2", APPLICATION_X_BZIP2);
          put("boz", APPLICATION_X_BZIP2);
          put("cbr", APPLICATION_X_CBR);
          put("cba", APPLICATION_X_CBR);
          put("cbt", APPLICATION_X_CBR);
          put("cbz", APPLICATION_X_CBR);
          put("cb7", APPLICATION_X_CBR);
          put("vcd", APPLICATION_X_CDLINK);
          put("cfs", APPLICATION_X_CFS_COMPRESSED);
          put("chat", APPLICATION_X_CHAT);
          put("pgn", APPLICATION_X_CHESS_PGN);
          put("nsc", APPLICATION_X_CONFERENCE);
          put("cpio", APPLICATION_X_CPIO);
          put("csh", APPLICATION_X_CSH);
          put("deb", APPLICATION_X_DEBIAN_PACKAGE);
          put("udeb", APPLICATION_X_DEBIAN_PACKAGE);
          put("dgc", APPLICATION_X_DGC_COMPRESSED);
          put("dir", APPLICATION_X_DIRECTOR);
          put("dcr", APPLICATION_X_DIRECTOR);
          put("dxr", APPLICATION_X_DIRECTOR);
          put("cst", APPLICATION_X_DIRECTOR);
          put("cct", APPLICATION_X_DIRECTOR);
          put("cxt", APPLICATION_X_DIRECTOR);
          put("w3d", APPLICATION_X_DIRECTOR);
          put("fgd", APPLICATION_X_DIRECTOR);
          put("swa", APPLICATION_X_DIRECTOR);
          put("wad", APPLICATION_X_DOOM);
          put("ncx", APPLICATION_X_DTBNCX_XML);
          put("dtb", APPLICATION_X_DTBOOK_XML);
          put("res", APPLICATION_X_DTBRESOURCE_XML);
          put("dvi", APPLICATION_X_DVI);
          put("evy", APPLICATION_X_ENVOY);
          put("eva", APPLICATION_X_EVA);
          put("bdf", APPLICATION_X_FONT_BDF);
          put("gsf", APPLICATION_X_FONT_GHOSTSCRIPT);
          put("psf", APPLICATION_X_FONT_LINUX_PSF);
          put("otf", APPLICATION_X_FONT_OTF);
          put("pcf", APPLICATION_X_FONT_PCF);
          put("snf", APPLICATION_X_FONT_SNF);
          put("ttf", APPLICATION_X_FONT_TTF);
          put("ttc", APPLICATION_X_FONT_TTF);
          put("pfa", APPLICATION_X_FONT_TYPE1);
          put("pfb", APPLICATION_X_FONT_TYPE1);
          put("pfm", APPLICATION_X_FONT_TYPE1);
          put("afm", APPLICATION_X_FONT_TYPE1);
          put("woff", APPLICATION_X_FONT_WOFF);
          put("arc", APPLICATION_X_FREEARC);
          put("spl", APPLICATION_X_FUTURESPLASH);
          put("gca", APPLICATION_X_GCA_COMPRESSED);
          put("ulx", APPLICATION_X_GLULX);
          put("gnumeric", APPLICATION_X_GNUMERIC);
          put("gramps", APPLICATION_X_GRAMPS_XML);
          put("gtar", APPLICATION_X_GTAR);
          put("hdf", APPLICATION_X_HDF);
          put("install", APPLICATION_X_INSTALL_INSTRUCTIONS);
          put("iso", APPLICATION_X_ISO9660_IMAGE);
          put("jnlp", APPLICATION_X_JAVA_JNLP_FILE);
          put("latex", APPLICATION_X_LATEX);
          put("lzh", APPLICATION_X_LZH_COMPRESSED);
          put("lha", APPLICATION_X_LZH_COMPRESSED);
          put("mie", APPLICATION_X_MIE);
          put("prc", APPLICATION_X_MOBIPOCKET_EBOOK);
          put("mobi", APPLICATION_X_MOBIPOCKET_EBOOK);
          put("application", APPLICATION_X_MS_APPLICATION);
          put("lnk", APPLICATION_X_MS_SHORTCUT);
          put("wmd", APPLICATION_X_MS_WMD);
          put("wmz", APPLICATION_X_MS_WMZ);
          put("xbap", APPLICATION_X_MS_XBAP);
          put("mdb", APPLICATION_X_MSACCESS);
          put("obd", APPLICATION_X_MSBINDER);
          put("crd", APPLICATION_X_MSCARDFILE);
          put("clp", APPLICATION_X_MSCLIP);
          put("exe", APPLICATION_X_MSDOWNLOAD);
          put("dll", APPLICATION_X_MSDOWNLOAD);
          put("com", APPLICATION_X_MSDOWNLOAD);
          put("bat", APPLICATION_X_MSDOWNLOAD);
          put("msi", APPLICATION_X_MSDOWNLOAD);
          put("mvb", APPLICATION_X_MSMEDIAVIEW);
          put("m13", APPLICATION_X_MSMEDIAVIEW);
          put("m14", APPLICATION_X_MSMEDIAVIEW);
          put("wmf", APPLICATION_X_MSMETAFILE);
          // put("wmz", APPLICATION_X_MSMETAFILE);
          put("emf", APPLICATION_X_MSMETAFILE);
          put("emz", APPLICATION_X_MSMETAFILE);
          put("mny", APPLICATION_X_MSMONEY);
          put("pub", APPLICATION_X_MSPUBLISHER);
          put("scd", APPLICATION_X_MSSCHEDULE);
          put("trm", APPLICATION_X_MSTERMINAL);
          put("wri", APPLICATION_X_MSWRITE);
          put("nc", APPLICATION_X_NETCDF);
          put("cdf", APPLICATION_X_NETCDF);
          put("nzb", APPLICATION_X_NZB);
          put("p12", APPLICATION_X_PKCS12);
          put("pfx", APPLICATION_X_PKCS12);
          put("p7b", APPLICATION_X_PKCS7_CERTIFICATES);
          put("spc", APPLICATION_X_PKCS7_CERTIFICATES);
          put("p7r", APPLICATION_X_PKCS7_CERTREQRESP);
          put("rar", APPLICATION_X_RAR_COMPRESSED);
          put("ris", APPLICATION_X_RESEARCH_INFO_SYSTEMS);
          put("sh", APPLICATION_X_SH);
          put("shar", APPLICATION_X_SHAR);
          put("swf", APPLICATION_X_SHOCKWAVE_FLASH);
          put("xap", APPLICATION_X_SILVERLIGHT_APP);
          put("sql", APPLICATION_X_SQL);
          put("sit", APPLICATION_X_STUFFIT);
          put("sitx", APPLICATION_X_STUFFITX);
          put("srt", APPLICATION_X_SUBRIP);
          put("sv4cpio", APPLICATION_X_SV4CPIO);
          put("sv4crc", APPLICATION_X_SV4CRC);
          put("t3", APPLICATION_X_T3VM_IMAGE);
          put("gam", APPLICATION_X_TADS);
          put("tar", APPLICATION_X_TAR);
          put("tcl", APPLICATION_X_TCL);
          put("tex", APPLICATION_X_TEX);
          put("tfm", APPLICATION_X_TEX_TFM);
          put("texinfo", APPLICATION_X_TEXINFO);
          put("texi", APPLICATION_X_TEXINFO);
          put("obj", APPLICATION_X_TGIF);
          put("ustar", APPLICATION_X_USTAR);
          put("src", APPLICATION_X_WAIS_SOURCE);
          put("der", APPLICATION_X_X509_CA_CERT);
          put("crt", APPLICATION_X_X509_CA_CERT);
          put("fig", APPLICATION_X_XFIG);
          put("xlf", APPLICATION_X_XLIFF_XML);
          put("xpi", APPLICATION_X_XPINSTALL);
          put("xz", APPLICATION_X_XZ);
          put("z1", APPLICATION_X_ZMACHINE);
          put("z2", APPLICATION_X_ZMACHINE);
          put("z3", APPLICATION_X_ZMACHINE);
          put("z4", APPLICATION_X_ZMACHINE);
          put("z5", APPLICATION_X_ZMACHINE);
          put("z6", APPLICATION_X_ZMACHINE);
          put("z7", APPLICATION_X_ZMACHINE);
          put("z8", APPLICATION_X_ZMACHINE);
          put("xaml", APPLICATION_XAML_XML);
          put("xdf", APPLICATION_XCAP_DIFF_XML);
          put("xenc", APPLICATION_XENC_XML);
          put("xhtml", APPLICATION_XHTML_XML);
          put("xht", APPLICATION_XHTML_XML);
          put("xml", APPLICATION_XML);
          put("xsl", APPLICATION_XML);
          put("dtd", APPLICATION_XML_DTD);
          put("xop", APPLICATION_XOP_XML);
          put("xpl", APPLICATION_XPROC_XML);
          put("xslt", APPLICATION_XSLT_XML);
          put("xspf", APPLICATION_XSPF_XML);
          put("mxml", APPLICATION_XV_XML);
          put("xhvml", APPLICATION_XV_XML);
          put("xvml", APPLICATION_XV_XML);
          put("xvm", APPLICATION_XV_XML);
          put("yang", APPLICATION_YANG);
          put("yin", APPLICATION_YIN_XML);
          put("zip", APPLICATION_ZIP);
          put("adp", AUDIO_ADPCM);
          put("au", AUDIO_BASIC);
          put("snd", AUDIO_BASIC);
          put("mid", AUDIO_MIDI);
          put("midi", AUDIO_MIDI);
          put("kar", AUDIO_MIDI);
          put("rmi", AUDIO_MIDI);
          put("mp4a", AUDIO_MP4);
          put("mpga", AUDIO_MPEG);
          put("mp2", AUDIO_MPEG);
          put("mp2a", AUDIO_MPEG);
          put("mp3", AUDIO_MPEG);
          put("m2a", AUDIO_MPEG);
          put("m3a", AUDIO_MPEG);
          put("oga", AUDIO_OGG);
          put("ogg", AUDIO_OGG);
          put("spx", AUDIO_OGG);
          put("s3m", AUDIO_S3M);
          put("sil", AUDIO_SILK);
          put("uva", AUDIO_VND_DECE_AUDIO);
          put("uvva", AUDIO_VND_DECE_AUDIO);
          put("eol", AUDIO_VND_DIGITAL_WINDS);
          put("dra", AUDIO_VND_DRA);
          put("dts", AUDIO_VND_DTS);
          put("dtshd", AUDIO_VND_DTS_HD);
          put("lvp", AUDIO_VND_LUCENT_VOICE);
          put("pya", AUDIO_VND_MS_PLAYREADY_MEDIA_PYA);
          put("ecelp4800", AUDIO_VND_NUERA_ECELP4800);
          put("ecelp7470", AUDIO_VND_NUERA_ECELP7470);
          put("ecelp9600", AUDIO_VND_NUERA_ECELP9600);
          put("rip", AUDIO_VND_RIP);
          put("weba", AUDIO_WEBM);
          put("aac", AUDIO_X_AAC);
          put("aif", AUDIO_X_AIFF);
          put("aiff", AUDIO_X_AIFF);
          put("aifc", AUDIO_X_AIFF);
          put("caf", AUDIO_X_CAF);
          put("flac", AUDIO_X_FLAC);
          put("mka", AUDIO_X_MATROSKA);
          put("m3u", AUDIO_X_MPEGURL);
          put("wax", AUDIO_X_MS_WAX);
          put("wma", AUDIO_X_MS_WMA);
          put("ram", AUDIO_X_PN_REALAUDIO);
          put("ra", AUDIO_X_PN_REALAUDIO);
          put("rmp", AUDIO_X_PN_REALAUDIO_PLUGIN);
          put("wav", AUDIO_X_WAV);
          put("xm", AUDIO_XM);
          put("cdx", CHEMICAL_X_CDX);
          put("cif", CHEMICAL_X_CIF);
          put("cmdf", CHEMICAL_X_CMDF);
          put("cml", CHEMICAL_X_CML);
          put("csml", CHEMICAL_X_CSML);
          put("xyz", CHEMICAL_X_XYZ);
          put("bmp", IMAGE_BMP);
          put("cgm", IMAGE_CGM);
          put("g3", IMAGE_G3FAX);
          put("gif", IMAGE_GIF);
          put("ief", IMAGE_IEF);
          put("jpeg", IMAGE_JPEG);
          put("jpg", IMAGE_JPEG);
          put("jpe", IMAGE_JPEG);
          put("ktx", IMAGE_KTX);
          put("png", IMAGE_PNG);
          put("btif", IMAGE_PRS_BTIF);
          put("sgi", IMAGE_SGI);
          put("svg", IMAGE_SVG_XML);
          put("svgz", IMAGE_SVG_XML);
          put("tiff", IMAGE_TIFF);
          put("tif", IMAGE_TIFF);
          put("psd", IMAGE_VND_ADOBE_PHOTOSHOP);
          put("uvi", IMAGE_VND_DECE_GRAPHIC);
          put("uvvi", IMAGE_VND_DECE_GRAPHIC);
          put("uvg", IMAGE_VND_DECE_GRAPHIC);
          put("uvvg", IMAGE_VND_DECE_GRAPHIC);
          put("sub", IMAGE_VND_DVB_SUBTITLE);
          put("djvu", IMAGE_VND_DJVU);
          put("djv", IMAGE_VND_DJVU);
          put("dwg", IMAGE_VND_DWG);
          put("dxf", IMAGE_VND_DXF);
          put("fbs", IMAGE_VND_FASTBIDSHEET);
          put("fpx", IMAGE_VND_FPX);
          put("fst", IMAGE_VND_FST);
          put("mmr", IMAGE_VND_FUJIXEROX_EDMICS_MMR);
          put("rlc", IMAGE_VND_FUJIXEROX_EDMICS_RLC);
          put("mdi", IMAGE_VND_MS_MODI);
          put("wdp", IMAGE_VND_MS_PHOTO);
          put("npx", IMAGE_VND_NET_FPX);
          put("wbmp", IMAGE_VND_WAP_WBMP);
          put("xif", IMAGE_VND_XIFF);
          put("webp", IMAGE_WEBP);
          put("3ds", IMAGE_X_3DS);
          put("ras", IMAGE_X_CMU_RASTER);
          put("cmx", IMAGE_X_CMX);
          put("fh", IMAGE_X_FREEHAND);
          put("fhc", IMAGE_X_FREEHAND);
          put("fh4", IMAGE_X_FREEHAND);
          put("fh5", IMAGE_X_FREEHAND);
          put("fh7", IMAGE_X_FREEHAND);
          put("ico", IMAGE_X_ICON);
          put("sid", IMAGE_X_MRSID_IMAGE);
          put("pcx", IMAGE_X_PCX);
          put("pic", IMAGE_X_PICT);
          put("pct", IMAGE_X_PICT);
          put("pnm", IMAGE_X_PORTABLE_ANYMAP);
          put("pbm", IMAGE_X_PORTABLE_BITMAP);
          put("pgm", IMAGE_X_PORTABLE_GRAYMAP);
          put("ppm", IMAGE_X_PORTABLE_PIXMAP);
          put("rgb", IMAGE_X_RGB);
          put("tga", IMAGE_X_TGA);
          put("xbm", IMAGE_X_XBITMAP);
          put("xpm", IMAGE_X_XPIXMAP);
          put("xwd", IMAGE_X_XWINDOWDUMP);
          put("eml", MESSAGE_RFC822);
          put("mime", MESSAGE_RFC822);
          put("igs", MODEL_IGES);
          put("iges", MODEL_IGES);
          put("msh", MODEL_MESH);
          put("mesh", MODEL_MESH);
          put("silo", MODEL_MESH);
          put("dae", MODEL_VND_COLLADA_XML);
          put("dwf", MODEL_VND_DWF);
          put("gdl", MODEL_VND_GDL);
          put("gtw", MODEL_VND_GTW);
          put("mts", MODEL_VND_MTS);
          put("vtu", MODEL_VND_VTU);
          put("wrl", MODEL_VRML);
          put("vrml", MODEL_VRML);
          put("x3db", MODEL_X3D_BINARY);
          put("x3dbz", MODEL_X3D_BINARY);
          put("x3dv", MODEL_X3D_VRML);
          put("x3dvz", MODEL_X3D_VRML);
          put("x3d", MODEL_X3D_XML);
          put("x3dz", MODEL_X3D_XML);
          put("appcache", TEXT_CACHE_MANIFEST);
          put("ics", TEXT_CALENDAR);
          put("ifb", TEXT_CALENDAR);
          put("css", TEXT_CSS);
          put("csv", TEXT_CSV);
          put("html", TEXT_HTML);
          put("htm", TEXT_HTML);
          put("n3", TEXT_N3);
          put("txt", TEXT_PLAIN);
          put("text", TEXT_PLAIN);
          put("conf", TEXT_PLAIN);
          put("def", TEXT_PLAIN);
          put("list", TEXT_PLAIN);
          put("log", TEXT_PLAIN);
          put("in", TEXT_PLAIN);
          put("dsc", TEXT_PRS_LINES_TAG);
          put("rtx", TEXT_RICHTEXT);
          put("sgml", TEXT_SGML);
          put("sgm", TEXT_SGML);
          put("tsv", TEXT_TAB_SEPARATED_VALUES);
          put("t", TEXT_TROFF);
          put("tr", TEXT_TROFF);
          put("roff", TEXT_TROFF);
          put("man", TEXT_TROFF);
          put("me", TEXT_TROFF);
          put("ms", TEXT_TROFF);
          put("ttl", TEXT_TURTLE);
          put("uri", TEXT_URI_LIST);
          put("uris", TEXT_URI_LIST);
          put("urls", TEXT_URI_LIST);
          put("vcard", TEXT_VCARD);
          put("curl", TEXT_VND_CURL);
          put("dcurl", TEXT_VND_CURL_DCURL);
          put("scurl", TEXT_VND_CURL_SCURL);
          put("mcurl", TEXT_VND_CURL_MCURL);
          put("fly", TEXT_VND_FLY);
          put("flx", TEXT_VND_FMI_FLEXSTOR);
          put("gv", TEXT_VND_GRAPHVIZ);
          put("3dml", TEXT_VND_IN3D_3DML);
          put("spot", TEXT_VND_IN3D_SPOT);
          put("jad", TEXT_VND_SUN_J2ME_APP_DESCRIPTOR);
          put("wml", TEXT_VND_WAP_WML);
          put("wmls", TEXT_VND_WAP_WMLSCRIPT);
          put("s", TEXT_X_ASM);
          put("asm", TEXT_X_ASM);
          put("c", TEXT_X_C);
          put("cc", TEXT_X_C);
          put("cxx", TEXT_X_C);
          put("cpp", TEXT_X_C);
          put("h", TEXT_X_C);
          put("hh", TEXT_X_C);
          put("dic", TEXT_X_C);
          put("f", TEXT_X_FORTRAN);
          put("for", TEXT_X_FORTRAN);
          put("f77", TEXT_X_FORTRAN);
          put("f90", TEXT_X_FORTRAN);
          put("java", TEXT_X_JAVA_SOURCE);
          put("opml", TEXT_X_OPML);
          put("p", TEXT_X_PASCAL);
          put("pas", TEXT_X_PASCAL);
          put("nfo", TEXT_X_NFO);
          put("etx", TEXT_X_SETEXT);
          put("sfv", TEXT_X_SFV);
          put("uu", TEXT_X_UUENCODE);
          put("vcs", TEXT_X_VCALENDAR);
          put("vcf", TEXT_X_VCARD);
          put("3gp", VIDEO_3GPP);
          put("3g2", VIDEO_3GPP2);
          put("h261", VIDEO_H261);
          put("h263", VIDEO_H263);
          put("h264", VIDEO_H264);
          put("jpgv", VIDEO_JPEG);
          put("jpm", VIDEO_JPM);
          put("jpgm", VIDEO_JPM);
          put("mj2", VIDEO_MJ2);
          put("mjp2", VIDEO_MJ2);
          put("mp4", VIDEO_MP4);
          put("mp4v", VIDEO_MP4);
          put("mpg4", VIDEO_MP4);
          put("mpeg", VIDEO_MPEG);
          put("mpg", VIDEO_MPEG);
          put("mpe", VIDEO_MPEG);
          put("m1v", VIDEO_MPEG);
          put("m2v", VIDEO_MPEG);
          put("ogv", VIDEO_OGG);
          put("qt", VIDEO_QUICKTIME);
          put("mov", VIDEO_QUICKTIME);
          put("uvh", VIDEO_VND_DECE_HD);
          put("uvvh", VIDEO_VND_DECE_HD);
          put("uvm", VIDEO_VND_DECE_MOBILE);
          put("uvvm", VIDEO_VND_DECE_MOBILE);
          put("uvp", VIDEO_VND_DECE_PD);
          put("uvvp", VIDEO_VND_DECE_PD);
          put("uvs", VIDEO_VND_DECE_SD);
          put("uvvs", VIDEO_VND_DECE_SD);
          put("uvv", VIDEO_VND_DECE_VIDEO);
          put("uvvv", VIDEO_VND_DECE_VIDEO);
          put("dvb", VIDEO_VND_DVB_FILE);
          put("fvt", VIDEO_VND_FVT);
          put("mxu", VIDEO_VND_MPEGURL);
          put("m4u", VIDEO_VND_MPEGURL);
          put("pyv", VIDEO_VND_MS_PLAYREADY_MEDIA_PYV);
          put("uvu", VIDEO_VND_UVVU_MP4);
          put("uvvu", VIDEO_VND_UVVU_MP4);
          put("viv", VIDEO_VND_VIVO);
          put("webm", VIDEO_WEBM);
          put("f4v", VIDEO_X_F4V);
          put("fli", VIDEO_X_FLI);
          put("flv", VIDEO_X_FLV);
          put("m4v", VIDEO_X_M4V);
          put("mkv", VIDEO_X_MATROSKA);
          put("mk3d", VIDEO_X_MATROSKA);
          put("mks", VIDEO_X_MATROSKA);
          put("mng", VIDEO_X_MNG);
          put("asf", VIDEO_X_MS_ASF);
          put("asx", VIDEO_X_MS_ASF);
          put("vob", VIDEO_X_MS_VOB);
          put("wm", VIDEO_X_MS_WM);
          put("wmv", VIDEO_X_MS_WMV);
          put("wmx", VIDEO_X_MS_WMX);
          put("wvx", VIDEO_X_MS_WVX);
          put("avi", VIDEO_X_MSVIDEO);
          put("movie", VIDEO_X_SGI_MOVIE);
          put("smv", VIDEO_X_SMV);
          put("ice", X_CONFERENCE_X_COOLTALK);
        }
      };

  public static String getByExtension(String extension) {
    return extToMIME.get(extension);
  }

  public static String getExtension(String mimeType) {
    return MIMEtoExt.get(mimeType);
  }
}
