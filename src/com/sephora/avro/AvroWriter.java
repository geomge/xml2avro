package com.sephora.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts internal TreeNode representation to avro format according to provided schema
 */
public class AvroWriter {

    //private static final Logger logger = LoggerFactory.getLogger(AvroWriter.class);

    private static Set<Schema.Type> primitiveTypes = new HashSet<>(Arrays.asList(Schema.Type.STRING, Schema.Type.INT,
            Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.BOOLEAN));

    private static Set<Schema.Type> complexTypes = new HashSet<>(Arrays.asList(Schema.Type.ARRAY, Schema.Type.RECORD));

    private Schema schema;


    public AvroWriter(InputStream schemaFile) throws IOException {
        schema = new Schema.Parser().parse(schemaFile);
    }

    /**
     * Writes TreeNode object as avro file
     * @param root TreeNode object to write
     * @param out output stream to write avro file
     */
    public void write(TreeNode root, OutputStream out) throws IOException {
        GenericRecord record = createRecord(root, schema);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, out);
        dataFileWriter.append(record);
        dataFileWriter.close();

        /* Schema-less encoding
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        */
    }

    /**
     * Extracts fields map from record type schema
     * @param schema schema to extract fields
     * @return map of fields: name -> value
     */
    private static Map<String, Schema.Field> getFieldsMap(Schema schema) {
        Map<String, Schema.Field> map = schema.getFields().stream()
                .collect(Collectors.toMap(Schema.Field::name, item -> item));
        return map;
    }

    /**
     * Extracts field names with default values from current layer of the schema
     * @param schema schema to check for default values
     * @return set of field names with default value
     */
    private static Set<String> getDefaultFields(Schema schema) {
        Set<String> fields = schema.getFields().stream()
                .filter(field -> field.defaultVal() != null)
                .map(field -> field.name())
                .collect(Collectors.toSet());
        return new HashSet<>(fields);
    }


    /**
     * Converts text representation of the timestamp to unix timestamp
     * @param text timestamp like '2018-05-09T14:00:28-07:00'
     * @return unix timestamp like 1531389292
     */
    public static long parseDateTime(String text) {
        Calendar c = DatatypeConverter.parseDateTime(text);
        c.setTimeZone(TimeZone.getTimeZone("UTC-0"));
        return c.getTimeInMillis();
    }

    /**
     * Converts TreeNode to avro Record
     * @param node TreeNode object to convert
     * @param schema schema that describes GenericRecord structure
     * @return GenericRecord representation
     */
    private static GenericRecord createRecord(TreeNode node, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);

        Map<String, Schema.Field> schemas = getFieldsMap(schema);

        Set<String> nodeKeys = node.getKeys();

        // avro can't populate default fields itself, handle all such values ourself.
        // Good point is that avro provides properly prepared object for default value, better than nothing
        Set<String> keys = getDefaultFields(schema);
        keys.addAll(nodeKeys);

        for (String key: keys) {
            if (!schemas.containsKey(key)) {
                //logger.error("Field '{}' isn't present in the schema and will be dropped", key);
                continue;
            }

            Schema.Field field = schemas.get(key);
            Schema fieldSchema = field.schema();
            Schema.Type schemaType = fieldSchema.getType();

            List<TreeNode> fieldsByKey = node.getValues(key);
            if (schemaType == Schema.Type.UNION) {
                fieldSchema = chooseSchema(fieldsByKey, fieldSchema.getTypes());
                schemaType = fieldSchema.getType();
            }

            //logger.debug("Start encoding for key='{}' and type='{}'", key, schemaType);
            if (!nodeKeys.contains(key)) {
                // if schema has default value for a field then we need only to put
                // it properly into the document
                record.put(key, field.defaultVal());
                continue;
            }

            Object avroValue = null;
            switch (schemaType) {
                case ARRAY:
                    avroValue = createArray(fieldsByKey, fieldSchema);
                    break;
                case RECORD:
                    avroValue = createRecord(node.getValue(key), fieldSchema);
                    break;
                default:
                    String data = node.getValue(key).getData();
                    avroValue = encodePrimitive(data, schemaType, key);
            }

            if (avroValue != null) {
                record.put(key, avroValue);
            }
            //logger.debug("End Encoding for key='{}' and type='{}'", key, schemaType);
        }
        return record;
    }

    private static Object encodePrimitive(String data, Schema.Type schemaType, String key) {
        Object avroValue = null;
        switch (schemaType) {
            case STRING:
                avroValue = data;
                break;

            case DOUBLE:
                avroValue = Double.parseDouble(data);
                break;

            case FLOAT:
                avroValue = Float.parseFloat(data);
                break;

            case LONG:
                avroValue = data.contains("T") ? parseDateTime(data) : Long.parseLong(data);
                break;

            case INT:
                avroValue = Integer.parseInt(data);
                break;

            case BOOLEAN:
                avroValue = Boolean.parseBoolean(data);
                break;

            case NULL:
                // usually it means union type and matching failed
                // get first schema and it's usually the null.
                // It's ok if we have empty field
                //logger.warn("Null data type for field '{}', possible data loss!", key);
                break;

            default:
                //logger.error("Unsupported type '{}' for key '{}'", schemaType, key);
                break;
        }
        return avroValue;
    }

    /**
     * Converts TreeNode objects to avro Array
     * @param nodes TreeNode objects to convert
     * @param schema schema that describes GenericArray structure
     * @return GenericArray representation
     */
    private static GenericArray<Object> createArray(List<TreeNode> nodes, Schema schema) {
        GenericData.Array<Object> array = new GenericData.Array<>(nodes.size(), schema);
        Schema itemSchema = schema.getElementType();
        Schema.Type schemaType = itemSchema.getType();

        switch (schemaType) {
            case RECORD:
                for (TreeNode node: nodes) {
                    array.add(createRecord(node, itemSchema));
                }
                break;

            default:
                for (TreeNode node: nodes) {
                    array.add(encodePrimitive(node.getData(), schemaType, null));
                }
                break;
        }

        return array;
    }

    /**
     * Checks TreeNode objects to choose appropriate schema from the list
     * @param nodes TreeNode objects to check
     * @param schemas schemas to choose from
     * @return most appropriate schema for gived data
     */
    private static Schema chooseSchema(List<TreeNode> nodes, List<Schema> schemas) {
        if (nodes.size() == 0) {
            // in case of default values we should use first schema
            return schemas.get(0);
        }

        for (Schema schema: schemas) {
            if (isAppropriateSchema(nodes.get(0), schema)) {
                return schema;
            }
        }
        // if we don't have appropriate schema, usually it means null
        // and if we have a null type it should be first
        return schemas.get(0);
    }

    /**
     * Checks compatibility of the one specific TreeNode object with one specific schema
     * @param node TreeNode object to check
     * @param schema schema to check
     * @return true if node compatible with current schema
     */
    private static boolean isAppropriateSchema(TreeNode node, Schema schema) {
        Schema.Type type = schema.getType();
        //logger.debug("Type matching data='{}', type='{}', keys='{}'", node.getData(), type.toString(), node.getKeys());

        // getting first primitive type beside null
        if (primitiveTypes.contains(type) && node.getData() != null) {
            return true;
        }
        // complex type matching, just first non primitive
        if (complexTypes.contains(type)) {
            return true;
        }

        return false;
    }
}
