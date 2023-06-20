package com.here.xyz.pub.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.naksha.lib.core.models.hub.Subscription;
import com.here.xyz.pub.mapper.DefaultPubMsgMapper;
import com.here.xyz.pub.mapper.IPubMsgMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class MessageUtil {
    private static final Logger logger = LogManager.getLogger();

    private static Map<String, IPubMsgMapper> instanceMap = new HashMap<>();
    final public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    public static IPubMsgMapper getMsgMapperInstance(final Subscription sub) {
        Map<String, Object> paramsMap = null;
        String msgFormat = null;
        // find out, which message format is requested
        if (sub == null
                || sub.getConfig() == null
                || (paramsMap = sub.getConfig().getParams()) == null
                || (msgFormat = paramsMap.get("msgFormat").toString()) == null
        ) {
            throw new RuntimeException("msgFormat not found for subId "+sub.getId());
        }

        // reuse mapper class from cache (if available)
        IPubMsgMapper msgMapper = instanceMap.get(msgFormat);
        if (msgMapper == null) {
            // create new mapper instance and add it in cache
            switch (msgFormat) {
                case "DEFAULT":
                    msgMapper = new DefaultPubMsgMapper();
                    break;
                default:
                    throw new RuntimeException("Unsupported msgFormat ["+msgFormat+"] for subscription id "+sub.getId());
            }
            instanceMap.put(msgFormat, msgMapper);
        }
        return msgMapper;
    }


    public static void addToAttributeMap(final Map<String, MessageAttributeValue> msgAttrMap,
                                         final String key, final String value) {
        msgAttrMap.put(key, MessageAttributeValue.builder().dataType("String").stringValue(value).build());
    }


    public static void addCustomFieldsToAttributeMap(final Map<String, MessageAttributeValue> msgAttrMap,
                                                     final Subscription sub, final String jsonData) {
        // Add custom message attributes as per subscription configuration
        if (sub.getConfig()!=null && sub.getConfig().getParams()!=null) {
            final Map<String, Object> attrMap = (Map<String, Object>) sub.getConfig().getParams().get("customMsgAttributes");
            if (attrMap!=null) {
                final Object document = Configuration.defaultConfiguration().jsonProvider().parse(jsonData); // parse one time per document
                for (final String attrKey : attrMap.keySet()) {
                    final Object jsonPathExpression = attrMap.get(attrKey);
                    if (jsonPathExpression!=null) {
                        try {
                            final String attrValue = JsonPath.read(document, jsonPathExpression.toString());
                            if (attrValue!=null && !attrValue.equals("")) {
                                addToAttributeMap(msgAttrMap, attrKey, attrValue);
                            }
                        } // skip this attribute in case it is not found
                        catch (PathNotFoundException ex) {
                            logger.trace("Exception extracting attribute [{}] using jsonpath [{}] from jsondata. ",
                                    attrKey, jsonPathExpression, ex);
                        }
                    }
                }

            }
        }
    }

    public static String compressAndEncodeToString(final String data) throws IOException {
        return encodeToString(compress(data.getBytes()));
    }

    public static byte[] compress(final byte[] msgBytes) throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipStream = new GZIPOutputStream(outStream);
        gzipStream.write(msgBytes);
        gzipStream.close();
        return outStream.toByteArray();
    }


    public static String encodeToString(final byte[] msgBytes) {
        return Base64.getEncoder().encodeToString(msgBytes);
    }


    /**
     * Converts given POJO into JSON String.
     *
     * Example,
     * <pre><code>
     * 	// Create POJO
     * 	User u = new User();
     * 	u.setName("John");
     * 	u.setSurname("Smith");
     *
     * 	// Convert POJO to JSON String
     * 	String jsonStr = MessageUtil.<b>toJson</b>(u);
     * </code></pre>
     *
     * @param object to be transformed to JSON string
     * @return the JSON string, or null in case of exception
     */
    public static String toJson(Object object) {
        String payloadJson = null;
        try {
            payloadJson = OBJECT_MAPPER.writeValueAsString(object);
        } catch (Exception ex) {
            payloadJson = null;
            logger.error("Exception converting Object to Json {} {}", object, ex.getMessage(), ex);
        }
        return payloadJson;
    }


    /**
     * Converts JSON string to Object of type specified as argument.
     *
     * Example,
     * <pre><code>
     * 	// Create JSON String
     * 	String payloadJson = "{ 'name' : 'John', 'surname' : 'Smith' }";
     *
     * 	// Convert JSON String to POJO of type User
     * 	User user = MessageUtil.<b>fromJson</b>(payloadJson, User.class);
     * </code></pre>
     *
     * @param <T> the generic type
     * @param payloadJson the JSON string data to be transformed to POJO
     * @param type the type of object to be returned
     * @return the transformed object of type T, or null in case of exception
     */
    public static <T> T fromJson(String payloadJson, Class<T> type) {
        T payloadObj = null;
        try {
            payloadObj = OBJECT_MAPPER.readValue(payloadJson, type);
        } catch (Exception ex) {
            payloadObj = null;
            logger.error("Exception converting Json to Object of type {} - {} , Error:{}", type, payloadJson, ex.getMessage(), ex);
        }
        return payloadObj;
    }



    /**
     * Converts JSON string to Object of type specified as argument. This is useful when type refers to generic, e.g. User&lt;Guest&gt; (instead of simple User.class).
     *
     * Example,
     * <pre><code>
     * 	// Create JSON String
     * 	String payloadJson = "{ 'name' : 'John', 'surname' : 'Smith' }";
     *
     * 	// Convert JSON String to POJO of type User&lt;Guest&gt;
     * 	TypeReference&lt;User&lt;Guest&gt;&gt; ref = new TypeReference&lt;User&lt;Guest&gt;&gt;() {};
     * 	User&lt;Guest&gt; guestUser = MessageUtil.<b>fromJson</b>(payloadJson, ref);
     * </code></pre>
     *
     * @param <T> the generic type
     * @param payloadJson the JSON string data to be transformed to POJO
     * @param ref the type of object to be returned
     * @return the transformed object of type T
     */
    public static <T> T fromJson(String payloadJson, TypeReference<T> ref) {
        T payloadObj = null;
        try {
            payloadObj = OBJECT_MAPPER.readValue(payloadJson, ref);
        } catch (Exception ex) {
            payloadObj = null;
            logger.error("Exception converting Json to Object of type {} - {}, Error:{}", ref.getClass().getComponentType(), payloadJson, ex.getMessage(), ex);
        }
        return payloadObj;
    }

}
