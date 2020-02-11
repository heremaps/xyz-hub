package com.here.xyz.hub.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.parser.ObjectMapperFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

  private static final List<String> removalTags = new ArrayList<>();

  private static String[] args;
  private static String src;
  private static String dest;
  private static JsonNode root;

  public static void main(String... arguments) {
    args = arguments;

    try {
      prepare();
      read();
      execute();
      write();
    } catch (Exception e) {
      System.out.println("OpenAPI tools:\n" +
          "Arguments: src <dest|STDOUT> " + VALID_OPTIONS);
    }
  }

  private static void prepare() throws Exception {
    src = args[0];
    dest = args[1];

    if (args.length > 2) {
      for (int i=2; i<args.length; i++) {
        final String stage = args[i];
        if (VALID_OPTIONS.contains(stage)) {
          removalTags.add(X + stage);
        }
      }
    }

    if (removalTags.isEmpty()) {
      throw new Exception();
    }
  }

  private static void read() throws Exception {
    // read the source in YAML format
    root = YAML_MAPPER.readTree(new File(src));
  }

  private static void execute() {
    removeTaggedObjects();
    replaceTaggedFieldnames();
    cleanupTaggedFieldnames();
    cleanupReferences();
    cleanupEmptyObjects();
  }

  private static void traverse(JsonNode node, Consumer<JsonNode> c) {
    if (node == null) return;
    c.accept(node);
    if (node.isContainerNode()) {
      for (JsonNode curr : node) {
        traverse(curr, c);
      }
    }
  }

  private static void removeTaggedObjects() {
    traverse(root, node -> {
      for (Iterator<JsonNode> it = node.iterator(); it.hasNext();) {
        JsonNode child = it.next();
        if (removalTags.stream().anyMatch(child::has)) {
          it.remove();
        }
      }
    });
  }

  private static void replaceTaggedFieldnames() {
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

  private static void cleanupTaggedFieldnames() {
    traverse(root, node -> {
      for (Iterator<String> it = node.fieldNames(); it.hasNext();) {
        String fieldname = it.next();
        if (VALID_TAGS.stream().anyMatch(fieldname::startsWith)) {
          it.remove();
        }
      }
    });
  }

  private static void cleanupReferences() {
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

  private static void cleanupEmptyObjects() {
    traverse(root, node -> {
      for (Entry<Object, JsonNode> entry : elements(node).entrySet()) {
        String fieldname = String.valueOf(entry.getKey());
        JsonNode child = entry.getValue();

        if (child.isContainerNode() && child.isEmpty()) {
          remove(node, fieldname);
        }
      }
    });
  }

  private static void remove(JsonNode node, String key) {
    if (node.isObject()) {
      ((ObjectNode) node).remove(key);
    } else if (node.isArray()) {
      ((ArrayNode) node).remove(Integer.parseInt(key));
    }
  }

  private static Map<Object, JsonNode> elements(JsonNode node) {
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

  private static void write() throws Exception {
    // write the results in YAML format
    String result = YAML_MAPPER.writeValueAsString(root);
    if (STDOUT.equals(dest)) {
      System.out.println(result);
    } else {
      try (FileWriter w = new FileWriter(dest)) {
        w.write(result);
      } catch (IOException e) {
        System.out.println(e.getMessage());
      }
    }
  }
}
