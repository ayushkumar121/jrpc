package jrpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import jrpc.ProtocolBuffers.Definition;
import jrpc.ProtocolBuffers.EnumDefinition;
import jrpc.ProtocolBuffers.MessageDefinition;
import jrpc.ProtocolBuffers.MessageField;
import jrpc.ProtocolBuffers.MessageFieldModifier;

public class MessageObject {
    private final Map<Integer, Object> fieldValues;
    private final Map<Integer, String> fieldNames;
    private final ProtocolBuffers.MessageDefinition definition;
    private final ProtocolBuffers pb;


    private static final int WIRE_TYPE_VARINT = 0;
    //private static final int WIRE_TYPE_FIXED64 = 1;
    private static final int WIRE_TYPE_LENGTH_DELIMITED = 2;
    /*private static final int WIRE_TYPE_START_GROUP = 3;
    private static final int WIRE_TYPE_END_GROUP = 4;
    private static final int WIRE_TYPE_FIXED32 = 5;
    */
    MessageObject(ProtocolBuffers pb, String name) throws Exception {
        this.pb = pb;
        Definition def = pb.getDefinitions().get(name);
        if (def == null || !(def instanceof MessageDefinition)) {
            throw new Exception("Unknown message definition: " + name);
        }
        this.definition = (MessageDefinition) def;
        this.fieldValues = new HashMap<>();
        this.fieldNames = new HashMap<>();
    }

    MessageObject(ProtocolBuffers pb, String name, InputStream in) throws Exception {
        this(pb, name);
        deserialize(in);
    }

    public void setField(String name, Object value) {
        Integer number = null;
        ProtocolBuffers.MessageField fieldDefinition = null;

        for (Map.Entry<Integer, ProtocolBuffers.MessageField> entry : definition.fields.entrySet()) {
            if (entry.getValue().identifier.equals(name)) {
                number = entry.getKey();
                fieldDefinition = entry.getValue();
                break;
            }
        }

        if (number == null) {
            throw new IllegalArgumentException("Unknown field: " + name);
        }

        if (fieldDefinition.modifier == MessageFieldModifier.REPEATED && value.getClass().isArray()) {
            throw new IllegalArgumentException("Field is repeated, but value is not an array");
        }

        if (fieldDefinition.modifier == MessageFieldModifier.MAP && !(value instanceof Map)) {
            throw new IllegalArgumentException("Field is map, but value is not a map");
        }


        String oneof = definition.oneofs.get(number);
        if (oneof != null) {
            for (Map.Entry<Integer, String> entry : definition.oneofs.entrySet()) {
                if (entry.getValue().equals(oneof)) {
                    fieldValues.remove(entry.getKey());
                    fieldNames.remove(entry.getKey());
                }
            }
        }

        fieldValues.put(number, value);
        fieldNames.put(number, name);
    }

    public Object getField(String name) {
        Integer number = null;

        for (Map.Entry<Integer, MessageField> entry : definition.fields.entrySet()) {
            if (entry.getValue().identifier.equals(name)) {
                number = entry.getKey();
                break;
            }
        }
        if (number == null) {
            throw new IllegalArgumentException("Unknown field: " + name);
        }

        return fieldValues.get(number);
    }

    public Map<Integer, Object> getFields() {
        return fieldValues;
    }

    public Map<Integer, String> getFieldNames() {
        return fieldNames;
    }

    public void serialize(OutputStream out) throws Exception {
        assertRequiredFields();

        for (Map.Entry<Integer, Object> entry : fieldValues.entrySet()) {
            Integer fieldNumber = entry.getKey();
            MessageField fieldDefinition = definition.fields.get(fieldNumber);

            if (fieldDefinition.modifier != MessageFieldModifier.OPTIONAL || entry.getValue() != null) {
                if (fieldDefinition.modifier == MessageFieldModifier.REPEATED) {
                    Object[] values = (Object[]) entry.getValue();
                    for (Object value : values) {
                        writeField(out, fieldNumber, fieldDefinition, value);
                    }
                } else {
                    writeField(out, fieldNumber, fieldDefinition, entry.getValue());
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(definition.identifier).append(" {");
        for (Map.Entry<Integer, Object> entry : fieldValues.entrySet()) {
            Integer fieldNumber = entry.getKey();
            String fieldName = fieldNames.get(fieldNumber);
            Object value = entry.getValue();
            sb.append("\n  ").append(fieldName).append(": ").append(value);
        }
        sb.append("\n}");
        return sb.toString();
    }

    private void writeVarint(OutputStream out, long value) throws Exception {
        while ((value & ~0x7F) != 0) {
            byte b = (byte) ((value & 0x7F) | 0x80);
            out.write(b);
            value >>>= 7;
        }
        out.write((byte) value);
    }

    private int readVarint(InputStream in) throws Exception {
        int result = 0;
        int shift = 0;
        while (true) {
            try {
                byte b = (byte) in.read();

                result |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
                shift += 7;
            } catch (IndexOutOfBoundsException e) {
                throw new Exception("Unexpected end of input");
            }

            if (shift > 35) {
                throw new Exception("Varint too long");
            }
        }
    }

    private void writeTag(OutputStream out, int fieldNumber, int wireType) throws Exception {
        writeVarint(out, (fieldNumber << 3) | wireType);
    }

    private void writeField(OutputStream out, Integer fieldNumber, MessageField fieldDefinition, Object value) throws Exception {
        switch (fieldDefinition.type) {
            case "int32" -> {
                writeTag(out, fieldNumber, WIRE_TYPE_VARINT);
                writeVarint(out, (int) value);
            }
            case "int64" -> {
                writeTag(out, fieldNumber, WIRE_TYPE_VARINT);
                writeVarint(out, (long) value);
            }
            case "bool" -> {
                writeTag(out, fieldNumber, WIRE_TYPE_VARINT);
                writeVarint(out, (boolean) value ? 1 : 0);
            }
            case "string" -> {
                writeTag(out, fieldNumber, WIRE_TYPE_LENGTH_DELIMITED);
                writeVarint(out, ((String) value).length());
                out.write(((String) value).getBytes());
            }
            case "bytes" -> {
                writeTag(out, fieldNumber, WIRE_TYPE_LENGTH_DELIMITED);
                writeVarint(out, ((byte[]) value).length);
                out.write(((byte[]) value));
            }
            default -> {
                Definition fieldTypeDefinition = pb.getDefinitions().get(fieldDefinition.type);
                if (fieldTypeDefinition == null) {
                    fieldTypeDefinition = pb.getDefinitions().get(definition.identifier + "." + fieldDefinition.type);
                }

                if (fieldTypeDefinition == null) {
                    throw new Exception("Unknown field definition for: " + fieldDefinition.identifier);
                }

                if (fieldTypeDefinition instanceof EnumDefinition) {
                    writeTag(out, fieldNumber, WIRE_TYPE_VARINT);
                    String enumValue = (String) value;
                    Integer enumNumber = ((EnumDefinition) fieldTypeDefinition).values.get(enumValue);
                    if (enumNumber == null) {
                        throw new Exception("Unknown enum value: " + enumValue);
                    }
                    writeVarint(out, enumNumber);
                } else if (fieldTypeDefinition instanceof MessageDefinition) {

                    if (fieldDefinition.modifier == MessageFieldModifier.MAP) {
                        MessageDefinition mapDefinition = ((MessageDefinition) fieldTypeDefinition);
                        if (value instanceof Map<?, ?>) {
                            Map<?, ?> mapObj = (Map<?, ?>) value;

                            for (Map.Entry<?, ?> entry : mapObj.entrySet()) {
                                MessageObject obj = new MessageObject(pb, mapDefinition.identifier);
                                obj.setField("key", entry.getKey());
                                obj.setField("value", entry.getValue());

                                ByteArrayOutputStream b = new ByteArrayOutputStream();
                                obj.serialize(b);

                                writeTag(out, fieldNumber, WIRE_TYPE_LENGTH_DELIMITED);
                                writeVarint(out, b.size());
                                out.write(b.toByteArray());
                            }
                        } else {
                            throw new Exception("Field is map, but value is not a map");
                        }
                    } else {
                        MessageObject obj = (MessageObject) value;
                        ByteArrayOutputStream b = new ByteArrayOutputStream();
                        obj.serialize(b);

                        writeTag(out, fieldNumber, WIRE_TYPE_LENGTH_DELIMITED);
                        writeVarint(out, b.size());
                        out.write(b.toByteArray());
                    }
                } else {
                    throw new Exception("Unknown field definition for: " + fieldDefinition.type);
                }
            }
        }
    }

    private void assertRequiredFields() throws Exception {
        for (Map.Entry<Integer, MessageField> entry : definition.fields.entrySet()) {
            Integer fieldNumber = entry.getKey();
            MessageField fieldDefinition = entry.getValue();
            Object value = fieldValues.get(fieldNumber);
            String oneof = definition.oneofs.get(fieldNumber);

            if (oneof != null) {
                boolean oneofSet = false;
                for (Map.Entry<Integer, String> oneOfEntry : definition.oneofs.entrySet()) {
                    if (oneOfEntry.getValue().equals(oneof) && fieldValues.containsKey(oneOfEntry.getKey())) {
                        oneofSet = true;
                        break;
                    }
                }

                if (!oneofSet) {
                    throw new Exception("Required oneof not set: " + oneof);
                }
            } else {
                if (value == null &&
                        fieldDefinition.modifier != MessageFieldModifier.OPTIONAL
                        && fieldDefinition.modifier != MessageFieldModifier.REPEATED) {
                    throw new Exception("Required field not set: " + fieldDefinition.identifier);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void deserialize(InputStream in) throws Exception {
        while (in.available() > 0) {
            long tag = readVarint(in);

            int fieldNumber = (int) (tag >> 3);
            int wireType = (int) (tag & 0x7);

            MessageField fieldDefinition = definition.fields.get(fieldNumber);
            if (fieldDefinition == null) {
                throw new Exception("Unknown field number: " + fieldNumber);
            }

            Object value;

            switch (wireType) {
                case WIRE_TYPE_VARINT -> value = readVarint(in);
                case WIRE_TYPE_LENGTH_DELIMITED -> {
                    int length = readVarint(in);
                    value = in.readNBytes(length);
                }
                default -> throw new Exception("Unknown wire type: " + wireType);
            }

            switch (fieldDefinition.type) {
                case "int32":
                case "int64":
                case "bytes":
                    break;
                case "bool":
                    value = (int) value != 0;
                    break;
                case "string":
                    assert value instanceof byte[];
                    value = new String((byte[]) value);
                    break;
                default: {
                    Definition fieldTypeDefinition = pb.getDefinitions().get(fieldDefinition.type);
                    if (fieldTypeDefinition == null) {
                        fieldTypeDefinition = pb.getDefinitions().get(definition.identifier + "." + fieldDefinition.type);
                    }

                    if (fieldTypeDefinition == null) {
                        throw new Exception("Unknown field definition for: " + fieldDefinition.identifier);
                    }

                    if (!(fieldTypeDefinition instanceof EnumDefinition)) {
                        if (fieldTypeDefinition instanceof MessageDefinition) {
                            MessageObject obj = new MessageObject(pb, fieldDefinition.type);
                            assert value instanceof byte[];
                            ByteArrayInputStream a = new ByteArrayInputStream((byte[]) value);
                            obj.deserialize(a);
                            value = obj;
                        } else {
                            throw new Exception("Unknown field definition for: " + fieldDefinition.identifier);
                        }
                    }
                }
            }

            if (fieldDefinition.modifier == MessageFieldModifier.REPEATED) {
                if (fieldValues.containsKey(fieldNumber) ) {
                    Object existingValue = fieldValues.get(fieldNumber);
                    Object[] values = (Object[]) existingValue;
                    Object[] newValues = new Object[values.length + 1];

                    System.arraycopy(values, 0, newValues, 0, values.length);
                    newValues[values.length] = value;
                    fieldValues.put(fieldNumber, newValues);
                } else {
                    fieldValues.put(fieldNumber, new Object[]{value});
                }
            } else if (fieldDefinition.modifier == MessageFieldModifier.MAP) {
                MessageObject obj = (MessageObject) value;

                Object key = obj.getField("key");
                Object mapValue = obj.getField("value");

                if (fieldValues.containsKey(fieldNumber)) {
                    Map<Object, Object> map = (Map<Object, Object>) fieldValues.get(fieldNumber);
                    map.put(key, mapValue);
                } else {
                    Map<Object, Object> map = new HashMap<>();
                    map.put(key, mapValue);
                    fieldValues.put(fieldNumber, map);
                }
            } else {
                fieldValues.put(fieldNumber, value);
            }

            fieldNames.put(fieldNumber, fieldDefinition.identifier);
        }

        assertRequiredFields();
    }
}
