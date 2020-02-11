package com.here.xyz.hub.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.parser.ObjectMapperFactory;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class OpenApiTransformer {

  private static final ObjectMapper YAML_MAPPER = ObjectMapperFactory.createYaml();

  private static final String X = "x";
  private static final String DASH = "-";
  private static final String STDOUT = "STDOUT";
  private static final String REF = "$ref";
  private static final String REF_START = "#/";
  private static final List<String> VALID_OPTIONS = Arrays.asList("-experimental","-deprecated");
  private static final List<String> VALID_TAGS = VALID_OPTIONS.stream().map(v->X+v).collect(Collectors.toList());
  private static final List<String> VALID_FIELDNAMES = VALID_TAGS.stream().map(v->v+DASH).collect(Collectors.toList());

  private JsonNode root;

  private final InputStream in;
  private final OutputStream out;
  private final List<String> removalTags = new ArrayList<>();

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

  private void read() throws Exception {
    // read the source in YAML format
    root = YAML_MAPPER.readTree(in);
  }

  private void execute() {
    removeTaggedObjects();
    replaceTaggedFieldnames();
    cleanupTaggedFieldnames();
    cleanupReferences();
    cleanupEmptyObjects();
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
      for (Entry<Object, JsonNode> entry : elements(node).entrySet()) {
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

  public static void main(String... args) {
    try {
      OpenApiTransformer OpenApiTransformer = prepare(args);
      OpenApiTransformer.transform();
    } catch (Exception e) {
      System.out.println("OpenAPI tools:\n" +
          "Arguments: src dest " + VALID_OPTIONS);
    }
  }

  private static OpenApiTransformer prepare(String... args) throws Exception {
    String src = args[0];
    String dest = args[1];

    String[] tags = new String[args.length-2];
    if (args.length > 2) {
      for (int i=2; i<args.length; i++) {
        final String stage = args[i];
        if (VALID_OPTIONS.contains(stage)) {
          tags[i-2] = X + stage;
        }
      }
    }

    try (FileInputStream fin = new FileInputStream(src); FileOutputStream fout = new FileOutputStream(dest)) {
      return new OpenApiTransformer(fin, fout, tags);
    }
  }
}
