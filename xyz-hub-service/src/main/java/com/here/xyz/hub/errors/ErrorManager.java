package com.here.xyz.hub.errors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.util.errors.ErrorDefinition;
import com.here.xyz.util.service.DetailedHttpException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErrorManager {

    private static final Logger logger = LogManager.getLogger();

    private static final String ERROR_DEFINITIONS_FILE = "errors.json";

    private static final Map<String, ErrorDefinition> errorMap = new HashMap<>();

    private static Map<String, String> defaultPlaceholders = new HashMap<>();

    public static void init(Map<String, String> defaults) {
        defaultPlaceholders = new HashMap<>(defaults);
        logger.info("Initializing ErrorManager with default placeholders: {}", defaultPlaceholders);
        loadErrors(ERROR_DEFINITIONS_FILE);
    }

    public static void init() {
        defaultPlaceholders = new HashMap<>();
        logger.info("Initializing ErrorManager without default placeholders.");
        loadErrors(ERROR_DEFINITIONS_FILE);
    }

    public static void loadErrors(String fileName) {
        logger.info("Loading error definitions from resource file: {}", fileName);
        try (InputStream inputStream = ErrorManager.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                String errMsg = fileName + " resource not found";
                logger.error(errMsg);
                throw new RuntimeException(errMsg);
            }
            ObjectMapper mapper = new ObjectMapper();
            List<ErrorDefinition> errors = mapper.readValue(inputStream, new TypeReference<List<ErrorDefinition>>() {
            });
            errors.forEach(error -> {
                errorMap.put(error.getCode(), error);
                logger.debug("Loaded error definition: code={}, title={}", error.getCode(), error.getTitle());
            });
            logger.info("Successfully loaded {} error definitions from resource: {}", errors.size(), fileName);
        } catch (Exception e) {
            logger.error("Failed to load error definitions from file: {}", fileName, e);
            throw new RuntimeException("Failed to load error definitions", e);
        }
    }

    public static void addDefaultPlaceholders(Map<String, String> placeholders) {
        defaultPlaceholders.putAll(placeholders);
        logger.info("Added default placeholders: {}", placeholders);
    }

    public static DetailedHttpException getHttpException(String errorCode) {
        return getHttpException(errorCode, null, null);
    }

    public static DetailedHttpException getHttpException(String errorCode, Throwable cause) {
        return getHttpException(errorCode, null, cause);
    }

    public static DetailedHttpException getHttpException(String errorCode, Map<String, String> placeholders) {
        return getHttpException(errorCode, placeholders, null);
    }

    public static DetailedHttpException getHttpException(String errorCode, Map<String, String> placeholders, Throwable cause) {
        ErrorDefinition errorDefinition = errorMap.get(errorCode);
        if (errorDefinition == null) {
            throw new IllegalArgumentException("Requested error code not found: " + errorCode);
        }
        String formattedTitle = format(errorDefinition.getTitle(), placeholders);
        String formattedCause = format(errorDefinition.getCause(), placeholders);
        String formattedAction = format(errorDefinition.getAction(), placeholders);

        return new DetailedHttpException(errorDefinition.withPlaceholders(formattedTitle, formattedCause, formattedAction), cause);
    }

    private static String format(String template, Map<String, String> placeholders) {
        String result = template;
        Map<String, String> effectivePlaceholders = new HashMap<>(defaultPlaceholders);
        if (placeholders != null) {
            effectivePlaceholders.putAll(placeholders);
        }
        for (Map.Entry<String, String> entry : effectivePlaceholders.entrySet()) {
            result = result.replace("$" + entry.getKey(), entry.getValue());
        }
        return result;
    }
}