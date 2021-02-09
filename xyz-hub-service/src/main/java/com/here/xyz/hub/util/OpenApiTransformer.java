package com.here.xyz.hub.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.here.xyz.hub.Service;
import io.swagger.v3.parser.ObjectMapperFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OpenApiTransformer {
  private static final Logger logger = LogManager.getLogger();
  private static final ObjectMapper YAML_MAPPER = ObjectMapperFactory.createYaml();

  private static final String X = "x";
  private static final String DASH = "-";
  private static final String REF = "$ref";
  private static final String REF_START = "#/";
  private static final List<String> VALID_OPTIONS = Arrays.asList("-experimental","-deprecated");
  private static final List<String> VALID_TAGS = VALID_OPTIONS.stream().map(v->X+v).collect(Collectors.toList());
  private static final List<String> VALID_FIELDNAMES = VALID_TAGS.stream().map(v->v+DASH).collect(Collectors.toList());

  private JsonNode root;

  private final InputStream in;
  private final List<String> removalTags = new ArrayList<>();
  private final OutputStream out;

  public String fullApi;
  public String stableApi;
  public String experimentalApi;
  public String contractApi;
  public String contractLocation;

  public OpenApiTransformer(InputStream in, OutputStream out, String... tags) {
    this.in = in;
    this.out = out;
    this.removalTags.addAll(Arrays.asList(tags));
  }

  public void transform() throws Exception {
    read();
    execute();
    write();
  }

  public void contract(Consumer<JsonNode> c) throws Exception {
    read();
    execute();
    patch(c);
    write();
  }

  private void read() throws Exception {
    // read the source in YAML format
    root = YAML_MAPPER.readTree(in);
  }

  private void execute() {
    setVersion();
    removeTaggedObjects();
    replaceTaggedFieldnames();
    cleanupTaggedFieldnames();
    cleanupReferences();
    cleanupEmptyObjects();
  }

  private void patch(Consumer<JsonNode> c) {
    c.accept(root);
  }

  private void traverse(JsonNode node, Consumer<JsonNode> c) {
    if (node == null) return;
    c.accept(node);
    if (node.isContainerNode()) {
      for (JsonNode curr : node) {
        traverse(curr, c);
      }
    }
  }

  private void setVersion() {
    ((ObjectNode)root.get("info")).put("version", Service.BUILD_VERSION);
  }

  private void removeTaggedObjects() {
    traverse(root, node -> {
      for (Iterator<JsonNode> it = node.iterator(); it.hasNext();) {
        JsonNode child = it.next();
        if (removalTags.stream().anyMatch(child::has)) {
          it.remove();
        }
      }
    });
  }

  private void replaceTaggedFieldnames() {
    traverse(root, node -> {
      for (Entry<Object, JsonNode> entry : elements(node).entrySet()) {
        String fieldname = String.valueOf(entry.getKey());
        if (VALID_FIELDNAMES.stream().anyMatch(fieldname::startsWith)) {
          if (removalTags.stream().noneMatch(fieldname::startsWith)) {
            final String replacement = VALID_FIELDNAMES.stream().reduce(fieldname, (res, el) -> res.replace(el, ""));
            ((ObjectNode) node).set(replacement, node.get(fieldname));
          }
        }
      }
    });
  }

  private void cleanupTaggedFieldnames() {
    traverse(root, node -> {
      for (Iterator<String> it = node.fieldNames(); it.hasNext();) {
        String fieldname = it.next();
        if (VALID_TAGS.stream().anyMatch(fieldname::startsWith)) {
          it.remove();
        }
      }
    });
  }

  private void cleanupReferences() {
    root.findParents(REF).forEach(parent -> {
      String[] ref = parent.get(REF).textValue().replace(REF_START, "").split("/");
      JsonNode curr = root;
      for (String s : ref) {
        curr = curr.get(s);
        if (curr == null) {
          ((ObjectNode) parent).remove(REF);
          break;
        }
      }
    });
  }

  private void cleanupEmptyObjects() {
    final List<String> securitySchemes = securitySchemes();

    traverse(root, node -> {
      final ArrayList<Entry<Object, JsonNode>> entries = new ArrayList<>(elements(node).entrySet());
      Collections.reverse(entries);

      for (Entry<Object, JsonNode> entry : entries) {
        String fieldname = String.valueOf(entry.getKey());
        JsonNode child = entry.getValue();

        // skip empty security schemes
        if (child.isArray() && child.isEmpty() && securitySchemes.contains(fieldname)) {
          continue;
        }

        if (child.isContainerNode() && child.isEmpty()) {
          remove(node, fieldname);
        }
      }
    });
  }

  private List<String> securitySchemes() {
    try {
      final List<String> result = new ArrayList<>();
      root.get("components").get("securitySchemes").fieldNames().forEachRemaining(result::add);
      return result;
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  private void remove(JsonNode node, String key) {
    if (node.isObject()) {
      ((ObjectNode) node).remove(key);
    } else if (node.isArray()) {
      ((ArrayNode) node).remove(Integer.parseInt(key));
    }
  }

  private Map<Object, JsonNode> elements(JsonNode node) {
    if (node == null) return Collections.emptyMap();

    final Map<Object, JsonNode> map = new HashMap<>();

    if (node.isObject()) {
      node.fieldNames().forEachRemaining((f) -> map.put(f, node.get(f)));
    } else if (node.isArray()) {
      int curr = 0;
      for (JsonNode jsonNode : node) {
        map.put(curr++, jsonNode);
      }
    }

    return map;
  }

  private void write() throws Exception {
    // write the results in YAML format
    String result = YAML_MAPPER.writeValueAsString(root);
    out.write(result.getBytes());
  }

  public static OpenApiTransformer generateAll() throws Exception {
    try (
        InputStream fin = Service.class.getResourceAsStream("/openapi.yaml");
        InputStream bin = new ByteArrayInputStream(IOUtils.toByteArray(fin));
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ) {
      final OpenApiTransformer transformer = new OpenApiTransformer(bin, bout);
      // put the original openapi.yaml in memory
      transformer.fullApi = IOUtils.toString(bin);

      // generate contract api
      bin.reset();
      transformer.contract(root -> {
        // fix the server url
        ((ObjectNode) root.get("servers").get(0)).put("url", "/");

        // fix the paths
        Map<String, JsonNode> paths = new LinkedHashMap<>();
        root.get("paths").fields().forEachRemaining(entry -> paths.put(entry.getKey(), entry.getValue()));
        ((ObjectNode) root.get("paths")).removeAll();
        paths.forEach((k, n) -> ((ObjectNode) root.get("paths")).set("/hub" + k, n));

        // fix the x-schema
        List<JsonNode> parents = root.get("components").get("requestBodies").findParents("schema");
        parents.forEach(parent -> {
          ObjectNode oParent = (ObjectNode) parent;
          oParent.set("x-schema", parent.get("schema"));
          oParent.remove("schema");
        });
      });
      transformer.contractApi = transformer.out.toString();

      // generate stable api
      bin.reset();
      bout.reset();
      transformer.removalTags.addAll(Arrays.asList("x-experimental", "x-deprecated"));
      transformer.transform();
      transformer.stableApi = transformer.out.toString();

      // generate experimental api
      bin.reset();
      bout.reset();
      transformer.removalTags.clear();
      transformer.removalTags.add("x-deprecated");
      transformer.transform();
      transformer.experimentalApi = transformer.out.toString();

      // store the contract location to be reused
      File tempFile = File.createTempFile("contract-", ".yaml");
      FileUtils.writeByteArrayToFile(tempFile, transformer.contractApi.getBytes());
      transformer.contractLocation = tempFile.toURI().toString();

      return transformer;
    } catch (Exception e) {
      logger.warn("Unable to transform openapi.yaml", e);
      throw new Exception("OpenApiTransformer failed to generate the api docs in memory.", e);
    }
  }

  public static void main(String... args) {
    try {
      final String[] validArgs = prepare(args);
      try(
          FileInputStream fin = new FileInputStream(validArgs[0]);
          FileOutputStream fout = new FileOutputStream(validArgs[1])
      ) {
        String[] tags = ArrayUtils.subarray(validArgs, 2, validArgs.length);
        new OpenApiTransformer(fin, fout, tags).transform();
      }
    } catch (Exception e) {
      System.out.println("OpenAPI tools:\n" +
          "Arguments: src dest " + VALID_OPTIONS);
    }
  }

  private static String[] prepare(String... args) {
    List<String> result = new ArrayList<String>() {{
      add(args[0]);
      add(args[1]);
    }};

    if (args.length > 2) {
      for (int i=2; i<args.length; i++) {
        final String stage = args[i];
        if (VALID_OPTIONS.contains(stage)) {
          result.add(X + stage);
        }
      }
    }

    return result.toArray(new String[0]);
  }
}
