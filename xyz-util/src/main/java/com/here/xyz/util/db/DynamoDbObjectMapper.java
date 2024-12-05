package com.here.xyz.util.db;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class DynamoDbObjectMapper {

    public static Map<String, Object> transformAttributeValueMap(Map<String, AttributeValue> attributeValueMap) {
        Map<String, Object> objectMap = new HashMap<>();
        for (Map.Entry<String, AttributeValue> entry : attributeValueMap.entrySet()) {
            String key = entry.getKey();
            AttributeValue attributeValue = entry.getValue();

            Object value;
            if (attributeValue.hasSs()) {
                value = attributeValue.s();
            } else if (attributeValue.hasNs()) {
                value = Double.parseDouble(attributeValue.n());
            } else if (attributeValue.hasBs()) {
                value = attributeValue.bool();
            } else if (attributeValue.hasL()) {
                value = attributeValue.l().stream()
                        .map(DynamoDbObjectMapper::convertAttributeValueToObject)
                        .collect(Collectors.toList());
            } else if (attributeValue.hasM()) {
                value = transformAttributeValueMap(attributeValue.m());
            } else {
                value = null;
            }

            objectMap.put(key, value);
        }
        return objectMap;
    }

    public static Object convertAttributeValueToObject(AttributeValue attributeValue) {
        if (attributeValue.hasSs()) {
            return attributeValue.s();
        } else if (attributeValue.hasNs()) {
            return Double.parseDouble(attributeValue.n());
        } else if (attributeValue.hasBs()) {
            return attributeValue.bool();
        } else if (attributeValue.hasL()) {
            return attributeValue.l().stream()
                    .map(DynamoDbObjectMapper::convertAttributeValueToObject)
                    .collect(Collectors.toList());
        } else if (attributeValue.hasM()) {
            return transformAttributeValueMap(attributeValue.m());
        }
        return null;
    }

    public static Map<String, AttributeValue> transformObjectMapToAttributeValueMap(Map<String, Object> objectMap) {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            AttributeValue attributeValue;

            if (value instanceof String) {
                attributeValue = AttributeValue.builder().s((String) value).build();
            } else if (value instanceof Number) {
                attributeValue = AttributeValue.builder().n(value.toString()).build();
            } else if (value instanceof Boolean) {
                attributeValue = AttributeValue.builder().bool((Boolean) value).build();
            } else if (value instanceof List) {
                List<AttributeValue> attributeValues = ((List<?>) value).stream()
                        .map(DynamoDbObjectMapper::convertObjectToAttributeValue)
                        .collect(Collectors.toList());
                attributeValue = AttributeValue.builder().l(attributeValues).build();
            } else if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nestedMap = (Map<String, Object>) value;
                attributeValue = AttributeValue.builder().m(transformObjectMapToAttributeValueMap(nestedMap)).build();
            } else {
                attributeValue = AttributeValue.builder().nul(true).build();
            }

            attributeValueMap.put(key, attributeValue);
        }

        return attributeValueMap;
    }

    public static AttributeValue convertObjectToAttributeValue(Object value) {
        if (value instanceof String) {
            return AttributeValue.builder().s((String) value).build();
        } else if (value instanceof Number) {
            return AttributeValue.builder().n(value.toString()).build();
        } else if (value instanceof Boolean) {
            return AttributeValue.builder().bool((Boolean) value).build();
        } else if (value instanceof List) {
            List<AttributeValue> attributeValues = ((List<?>) value).stream()
                    .map(DynamoDbObjectMapper::convertObjectToAttributeValue)
                    .collect(Collectors.toList());
            return AttributeValue.builder().l(attributeValues).build();
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> nestedMap = (Map<String, Object>) value;
            return AttributeValue.builder().m(transformObjectMapToAttributeValueMap(nestedMap)).build();
        } else {
            return AttributeValue.builder().nul(true).build();
        }
    }
}
