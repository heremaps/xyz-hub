package com.here.xyz.hub.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.parser.ObjectMapperFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class OpenApiGenerator {
  private static final String EXCLUDE = "exclude";
  private static final String INCLUDE = "include";
  private static final String REPLACE = "replace";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String TYPE = "type";
  private static final String FIND = "find";
  private static final String REGEX_SEARCH = "~=";
  private static final String EXACT_SEARCH = "=";

  private static final ObjectMapper YAML_MAPPER = ObjectMapperFactory.createYaml();

  private static JsonNode root;
  private static JsonNode recipe;

  /**
   * Process a yaml source by applying a yaml recipe in the top
   * which can have instructions to exclude, include or replace keys and values.
   * Result is a modified version of source.
   * @param sourceBytes the original content which will be used to generate the final version after modifications.
   * @param recipeBytes the recipe content which guides what will be excluded, included or replaced during the process.
   * @return an array of bytes which represents the modified source after process.
   * @throws Exception when something goes really wrong.
   */
  public static byte[] generate(byte[] sourceBytes, byte[] recipeBytes) throws Exception {
    root = YAML_MAPPER.readTree(sourceBytes);
    recipe = YAML_MAPPER.readTree(recipeBytes);

    processExclusions();
    processInclusions();
    processReplacements();

    // write the results in YAML format
    final String result = YAML_MAPPER.writeValueAsString(root);
    return result.getBytes();
  }

  /**
   * Iterate over the list of exclusions and compare whether the path exists or not within the input.
   * If it matches, excludes it from resulting output.
   * There are three ways to define an element to be excluded
   *   - by exact path
   *   - by exact value match
   *   - by regex value match
   * The first mode is a simple path match.
   * The last two modes will search only inside array of objects.
   */
  private static void processExclusions() {
    if (!recipe.has(EXCLUDE)) return;

    recipe.get(EXCLUDE).elements().forEachRemaining(e -> {
      final String excludeString = e.asText();
      String path = excludeString;
      String matcher = null;

      boolean regexSearch = false;
      boolean exactSearch = false;

      // set which kind of search to be used
      if (excludeString.contains(REGEX_SEARCH)) {
        // regex search
        final String[] splitString = excludeString.split(REGEX_SEARCH);
        path = splitString[0];
        matcher = splitString[1];
        regexSearch = true;
      } else if (excludeString.contains(EXACT_SEARCH)) {
        // exact matching search
        final String[] splitString = excludeString.split(EXACT_SEARCH);
        path = splitString[0];
        matcher = splitString[1];
        regexSearch = exactSearch = true;
      }

      final String[] pathKeys = split(path);
      JsonNode parent = root;

      // navigate to the last node's parent
      for (int i=0; i<pathKeys.length-1; i++) {
        parent = parent.get(pathKeys[i]);
      }

      final String lastKey = pathKeys[pathKeys.length - 1];
      if (!regexSearch) {
        remove(parent, lastKey);
      } else {
        for (Entry<Object, JsonNode> entry : elements(parent).entrySet()) {
          final JsonNode node = entry.getValue();
          if (node.isObject() && node.has(lastKey)) {
            final String value = node.get(lastKey).asText();
            if (exactSearch ? value.equals(matcher) : value.matches(matcher)) {
              remove(parent, String.valueOf(entry.getKey()));
            }
          }
        }
      }
    });
  }

  /**
   * Iterate over the list of inclusions and includes the "value" element in the specified "key".
   * It works with objects and arrays.
   * If parent is an object, value's keys are extracted and injected within the parent object.
   * If parent is an array, value is simply added to the end of the array.
   */
  private static void processInclusions() {
    if (!recipe.has(INCLUDE)) return;

    recipe.get(INCLUDE).elements().forEachRemaining(e -> {
      final String key = e.get(KEY).asText();
      final JsonNode value = e.get(VALUE);

      final String[] pathKeys = split(key);
      JsonNode parent = root;

      // navigate to the last node
      for (String pathKey : pathKeys) {
        parent = parent.get(pathKey);
      }

      if (parent.isObject()) {
        for (Entry<Object, JsonNode> entry : elements(value).entrySet()) {
          ((ObjectNode) parent).set(String.valueOf(entry.getKey()), entry.getValue());
        }
      } else if (parent.isArray()) {
        ((ArrayNode) parent).add(value);
      }
    });
  }

  /**
   * Iterates over the list of replacements and replaces the specified "find" element with the "replace" element.
   * Replacements can either be of types "key" or "value".
   * For "key" type, the last element in the path string is renamed to the value of "replace" field.
   * For "value" type, the child of the last element in the path string is replaced by the value of "replace" field.
   */
  private static void processReplacements() {
    if (!recipe.has(REPLACE)) return;

    recipe.get(REPLACE).elements().forEachRemaining(e -> {
      final String type = e.get(TYPE).asText();
      final String find = e.get(FIND).asText();
      final JsonNode replace = e.get(REPLACE);

      final String[] pathKeys = split(find);
      final String lastKey = pathKeys[pathKeys.length-1];
      JsonNode parent = root;


      // navigate to the last node's parent
      for (int i=0; i<pathKeys.length-1; i++) {
        parent = parent.get(pathKeys[i]);
      }

      switch (type) {
        case KEY: {
          if (!parent.isObject()) break;

          final String[] pathReplaceKeys = split(replace.asText());
          final String lastReplaceKey = pathReplaceKeys[pathReplaceKeys.length-1];

          final ObjectNode objectNode = (ObjectNode) parent;
          final JsonNode value = parent.get(lastKey);
          objectNode.remove(lastKey);
          objectNode.set(lastReplaceKey, value);

          break;
        }
        case VALUE: {
          if (parent.isObject()) {
            ((ObjectNode) parent).set(lastKey, replace);
          } else if (parent.isArray()) {
            ((ArrayNode) parent).set(Integer.parseInt(lastKey), replace);
          }

          break;
        }
      }
    });
  }

  /**
   * Splits the path string taking in consideration single quotes strings
   * @param path the string to be split
   * @return an array of strings where each element represents one hop in the path
   */
  private static String[] split(final String path) {
    if (StringUtils.isBlank(path)) return new String[0];

    final List<String> result = new ArrayList<>();
    final String[] pathSplit = path.split("\\.");
    String currNode = null;
    boolean store = true;

    for (final String val : pathSplit) {
      if (val.startsWith("'") && val.endsWith("'")) {
        currNode = val.split("'")[1];
      } else if (val.startsWith("'")) {
        store = false;
        currNode = val.substring(1);
      } else if (val.endsWith("'")) {
        store = true;
        currNode += "." + val.substring(0, val.length() - 1);
      } else if (!store) {
        currNode += "." + val;
      } else {
        currNode = val;
      }

      if (store) {
        result.add(currNode);
        currNode = null;
      }
    }

    return result.toArray(new String[0]);
  }

  /**
   * Removes a key from the parent which can be either Object or Array.
   * In case of array, key is converted to integer, which represents the index to be removed.
   * @param parent the parent object where the removal will occur.
   * @param key the key to be removed from parent.
   */
  private static void remove(JsonNode parent, String key) {
    if (parent.isObject()) {
      ((ObjectNode) parent).remove(key);
    } else if (parent.isArray()) {
      ((ArrayNode) parent).remove(Integer.parseInt(key));
    }
  }

  /**
   * Converts a JsonNode into a Map in which the keys are either Object's keys or Array's indices.
   * @param node the JsonNode object to be converted
   * @return A map of String|Integer-JsonNode
   */
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

  /**
   * Generates an open source specification by using a source and recipe files.
   * Results are saved into the output file.
   * e.g. <code>java com.here.xyz.hub.utilOpenApiGenerator &lt;source&gt; &lt;recipe&gt; &lt;recipe&gt;</code>
   * @param args source, recipe and output file names
   */
  public static void main(String... args) {
    try {
      if (args.length != 3) throw new Exception("Invalid number of parameters.");

      byte[] sourceBytes = FileUtils.readFileToByteArray(new File(args[0]));
      byte[] recipeBytes = FileUtils.readFileToByteArray(new File(args[1]));
      byte[] result = OpenApiGenerator.generate(sourceBytes, recipeBytes);

      FileUtils.writeByteArrayToFile(new File(args[2]), result);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      System.out.println("Usage: java OpenApiGenerator <source> <recipe> <output>");
    }
  }
}
