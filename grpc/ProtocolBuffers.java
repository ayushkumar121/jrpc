package grpc;

import java.io.File;
import java.nio.file.Files;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Optional;
import java.util.List;

public class ProtocolBuffers {
    public ProtocolBuffers(String filePath) throws Exception {
        definitions = new HashMap<>();
        importPaths = new ArrayList<>();

        parseProtocolBufferFile(filePath);
    }

    public MessageObject newMessageObject(String name) throws Exception {
        return new MessageObject(name);
    }

    public MessageObject newMessageObject(String name, byte[] data) throws Exception {
        return new MessageObject(name, data);
    }

    public class MessageObject {
        private Map<Integer, Object> fieldValues;
        private Map<Integer, String> fieldNames;
        private MessageDefinition definition;

        private static final int WIRE_TYPE_VARINT = 0;
        private static final int WIRE_TYPE_FIXED64 = 1;
        private static final int WIRE_TYPE_LENGTH_DELIMITED = 2;
        private static final int WIRE_TYPE_START_GROUP = 3;
        private static final int WIRE_TYPE_END_GROUP = 4;
        private static final int WIRE_TYPE_FIXED32 = 5;

        private MessageObject(String name) throws Exception {
            Definition def = definitions.get(name);
            if (!(def instanceof MessageDefinition)) {
                throw new Exception("Protobuf definition not found");
            }
            this.definition = (MessageDefinition) def;
            this.fieldValues = new HashMap<>();
            this.fieldNames = new HashMap<>();
        }

        private MessageObject(String name, byte[] data) throws Exception {
            this(name);
            deserialize(data);
        }

        public void setField(String name, Object value) {
            Integer number = null;
            MessageField fieldDefinition = null;

            for (Map.Entry<Integer, MessageField> entry : definition.fields.entrySet()) {
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

        private void writeVarint(Buffer out, long value) throws Exception {
            while ((value & ~0x7F) != 0) {
                byte b = (byte) ((value & 0x7F) | 0x80);
                out.write(b);
                value >>>= 7;
            }
            out.write((byte) value);
        }

        private int readVarint(Buffer in) throws Exception {
            int result = 0;
            int shift = 0;
            while (true) {
                try {
                    byte b = in.read();

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

        private void writeTag(Buffer out, int fieldNumber, int wireType) throws Exception {
            writeVarint(out, (fieldNumber << 3) | wireType);
        }

        private void writeField(Buffer out, Integer fieldNumber, MessageField fieldDefinition, Object value) throws Exception {
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
                    Definition fieldTypeDefinition = definitions.get(fieldDefinition.type);
                    if (fieldTypeDefinition == null) {
                        fieldTypeDefinition = definitions.get(definition.identifier + "." + fieldDefinition.type);
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
                            Map<Object, Object> mapObj = (Map) value;

                            for (Map.Entry<Object, Object> entry : mapObj.entrySet()) {
                                MessageObject obj = new MessageObject(mapDefinition.identifier);
                                obj.setField("key", entry.getKey());
                                obj.setField("value", entry.getValue());
                                byte[] bytes = obj.serialize();

                                writeTag(out, fieldNumber, WIRE_TYPE_LENGTH_DELIMITED);
                                writeVarint(out, bytes.length);
                                out.write(bytes);
                            }
                        } else {
                            MessageObject obj = (MessageObject) value;
                            byte[] bytes = obj.serialize();

                            writeTag(out, fieldNumber, WIRE_TYPE_LENGTH_DELIMITED);
                            writeVarint(out, bytes.length);
                            out.write(bytes);
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

        public byte[] serialize() throws Exception {
            assertRequiredFields();

            Buffer out = new Buffer();
            for (Map.Entry<Integer, Object> entry : fieldValues.entrySet()) {
                Integer fieldNumber = entry.getKey();
                MessageField fieldDefinition = definition.fields.get(fieldNumber);

                if (fieldDefinition.modifier == MessageFieldModifier.OPTIONAL && entry.getValue() == null) {
                    continue;
                } else if (fieldDefinition.modifier == MessageFieldModifier.REPEATED) {
                    Object[] values = (Object[]) entry.getValue();
                    for (Object value : values) {
                        writeField(out, fieldNumber, fieldDefinition, value);
                    }
                } else {
                    writeField(out, fieldNumber, fieldDefinition, entry.getValue());
                }
            }

            return out.getBytes();
        }

        private void deserialize(byte[] data) throws Exception {
            Buffer in = new Buffer(data);

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
                        value = in.read(length);
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
                        value = new String((byte[]) value);
                        break;
                    default: {
                        Definition fieldTypeDefinition = definitions.get(fieldDefinition.type);
                        if (fieldTypeDefinition == null) {
                            fieldTypeDefinition = definitions.get(definition.identifier + "." + fieldDefinition.type);
                        }

                        if (fieldTypeDefinition == null) {
                            throw new Exception("Unknown field definition for: " + fieldDefinition.identifier);
                        }

                        if (fieldTypeDefinition instanceof EnumDefinition) {
                            value = (int) value;
                        } else if (fieldTypeDefinition instanceof MessageDefinition) {
                            MessageObject obj = new MessageObject(fieldDefinition.type);
                            obj.deserialize((byte[]) value);
                            value = obj;
                        } else {
                            throw new Exception("Unknown field definition for: " + fieldDefinition.identifier);
                        }
                    }
                }

                fieldValues.put(fieldNumber, value);
                fieldNames.put(fieldNumber, fieldDefinition.identifier);
            }

            assertRequiredFields();
        }
    }

    // Private methods
    private enum Syntax {
        PROTO2,
        PROTO3,
    }

    private enum DefinitionType {
        MESSAGE,
        ENUM,
        SERVICE,
    }

    private interface Definition {
        DefinitionType getType();
    }

    private enum MessageFieldModifier {
        REPEATED,
        OPTIONAL,
        MAP,
    }

    private static class MessageField {
        public String identifier;
        public String type;
        public MessageFieldModifier modifier;
    }

    private static class MessageDefinition implements Definition {
        public String identifier;
        public Map<Integer, MessageField> fields;
        public Map<Integer, String> oneofs;

        @Override
        public DefinitionType getType() {
            return DefinitionType.MESSAGE;
        }
    }

    private static class EnumDefinition implements Definition {
        public Map<String, Integer> values;

        @Override
        public DefinitionType getType() {
            return DefinitionType.ENUM;
        }
    }

    private static class ServiceMethod {
        public String inputIdentifier;
        public String outputIdentifier;
    }

    private static class ServiceDefinition implements Definition {
        public String identifier;
        public Map<String, ServiceMethod> methods;

        @Override
        public DefinitionType getType() {
            return DefinitionType.SERVICE;
        }
    }

    public Syntax syntax;
    private List<String> importPaths;
    private Map<String, Definition> definitions;

    private String readEntireFile(String filePath) throws Exception {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new Exception("File not found");
        }

        byte[] data = Files.readAllBytes(file.toPath());
        return new String(data);
    }

    private enum TokenType {
        KEYWORD,
        EQUAL,
        STRING,
        IDENTIFIER,
        SEMICOLON,
        OPEN_BRACE,
        CLOSE_BRACE,
        OPEN_PAREN,
        CLOSE_PAREN,
        COMMA,
        COLON,
        NUMBER,
        OPEN_ANGLE_BRACKET,
        CLOSE_ANGLE_BRACKET,
        DOT,
    }

    private enum Keyword {
        SYNTAX("syntax"),
        IMPORT("import"),
        PUBLIC("public"),
        PACKAGE("package"),
        OPTION("option"),
        MESSAGE("message"),
        ENUM("enum"),
        MAP("map"),
        ONEOF("oneof"),
        SERVICE("service"),
        RPC("rpc"),
        RETURNS("returns"),
        REPEATED("repeated"),
        OPTIONAL("optional"),
        DOUBLE("double"),
        FLOAT("float"),
        INT32("int32"),
        INT64("int64"),
        UINT32("uint32"),
        UINT64("uint64"),
        SINT32("sint32"),
        SINT64("sint64"),
        FIXED32("fixed32"),
        FIXED64("fixed64"),
        SFIXED32("sfixed32"),
        SFIXED64("sfixed64"),
        STRING("string"),
        BOOL("bool"),
        BYTES("bytes"),
        REQUIRED("required"),
        ;

        public String value;

        private Keyword(String value) {
            this.value = value;
        }
    }

    private class Token {
        TokenType type;
        Keyword keyword;
        String value;
    }

    private boolean isPunctuation(char ch) {
        return ch == '<' ||
                ch == '>' ||
                ch == '=' ||
                ch == ';' ||
                ch == ',' ||
                ch == ':' ||
                ch == '(' ||
                ch == ')' ||
                ch == '{' ||
                ch == '}' ||
                ch == '.';
    }

    private Optional<Keyword> parseKeyword(String value) {
        for (Keyword keyword : Keyword.values()) {
            if (keyword.value.equals(value)) {
                return Optional.of(keyword);
            }
        }
        return Optional.empty();
    }

    private ArrayList<Token> tokenize(String input) {
        ArrayList<Token> tokens = new ArrayList<>();

        int i = 0;
        while (i < input.length()) {
            char ch = input.charAt(i);

            if (i + 1 < input.length() && input.charAt(i) == '/' && input.charAt(i + 1) == '/') {
                while (i < input.length() && input.charAt(i) != '\n') {
                    i++;
                }
            } else if (i + 1 < input.length() && input.charAt(i) == '/' && input.charAt(i + 1) == '*') {
                while (i < input.length() && input.charAt(i) != '*' && input.charAt(i + 1) != '/') {
                    i++;
                }
            } else if (Character.isWhitespace(ch)) {
                i++;
                continue;
            } else if (isPunctuation(ch)) {
                if (ch == ';') {
                    Token token = new Token();
                    token.type = TokenType.SEMICOLON;
                    tokens.add(token);
                } else if (ch == ',') {
                    Token token = new Token();
                    token.type = TokenType.COMMA;
                    tokens.add(token);
                } else if (ch == ':') {
                    Token token = new Token();
                    token.type = TokenType.COLON;
                    tokens.add(token);
                } else if (ch == '(') {
                    Token token = new Token();
                    token.type = TokenType.OPEN_PAREN;
                    tokens.add(token);
                } else if (ch == ')') {
                    Token token = new Token();
                    token.type = TokenType.CLOSE_PAREN;
                    tokens.add(token);
                } else if (ch == '{') {
                    Token token = new Token();
                    token.type = TokenType.OPEN_BRACE;
                    tokens.add(token);
                } else if (ch == '}') {
                    Token token = new Token();
                    token.type = TokenType.CLOSE_BRACE;
                    tokens.add(token);
                } else if (ch == '=') {
                    Token token = new Token();
                    token.type = TokenType.EQUAL;
                    tokens.add(token);
                } else if (ch == '<') {
                    Token token = new Token();
                    token.type = TokenType.OPEN_ANGLE_BRACKET;
                    tokens.add(token);
                } else if (ch == '>') {
                    Token token = new Token();
                    token.type = TokenType.CLOSE_ANGLE_BRACKET;
                    tokens.add(token);
                } else if (ch == '.') {
                    Token token = new Token();
                    token.type = TokenType.DOT;
                    tokens.add(token);
                }

                i++;
            } else if (ch == '\"') {
                i++;

                Token token = new Token();
                token.type = TokenType.STRING;

                StringBuilder sb = new StringBuilder();
                while (i < input.length() && input.charAt(i) != '\"') {
                    sb.append(input.charAt(i));
                    i++;
                }
                token.value = sb.toString();
                tokens.add(token);
                i++;
            } else if (Character.isDigit(ch)) {
                Token token = new Token();
                token.type = TokenType.NUMBER;

                StringBuilder sb = new StringBuilder();
                while (i < input.length() && (Character.isDigit(input.charAt(i)) || input.charAt(i) == '.')) {
                    sb.append(input.charAt(i));
                    i++;
                }
                token.value = sb.toString();
                tokens.add(token);
            } else {
                Token token = new Token();

                StringBuilder sb = new StringBuilder();
                while (i < input.length() && !Character.isWhitespace(input.charAt(i)) && !isPunctuation(input.charAt(i))) {
                    sb.append(input.charAt(i));
                    i++;
                }
                token.value = sb.toString();

                Optional<Keyword> keyword = parseKeyword(token.value);
                if (keyword.isPresent()) {
                    token.type = TokenType.KEYWORD;
                    token.keyword = keyword.get();
                } else {
                    token.type = TokenType.IDENTIFIER;
                }

                tokens.add(token);
            }
        }
        return tokens;
    }

    private class Parser {
        private final ArrayList<Token> tokens;
        private int index;

        public Parser(ArrayList<Token> tokens) {
            this.tokens = tokens;
            this.index = 0;
        }

        public boolean expect(TokenType type) {
            if (index < tokens.size() && tokens.get(index).type == type) {
                return true;
            }
            return false;
        }

        public Optional<Token> peek() {
            if (index < tokens.size()) {
                return Optional.of(tokens.get(index));
            }
            return Optional.empty();
        }

        public void consume() {
            if (index < tokens.size()) {
                index++;
            }
        }
    }

    private void parseSyntax(Parser parser) throws Exception {
        parser.consume();

        if (!parser.expect(TokenType.EQUAL)) {
            throw new Exception("Expected '='");
        }
        parser.consume();

        if (!parser.expect(TokenType.STRING)) {
            throw new Exception("Expected string");
        }

        String value = parser.peek().get().value;
        if (value.equals("proto3")) {
            syntax = Syntax.PROTO3;
        } else if (value.equals("proto2")) {
            syntax = Syntax.PROTO2;
        } else {
            throw new Exception("Invalid syntax: " + value);
        }
        parser.consume();

        if (!parser.expect(TokenType.SEMICOLON)) {
            throw new Exception("Expected ';'");
        }
        parser.consume();
    }

    private void parseImport(Parser parser) throws Exception {
        parser.consume();

        if (!parser.expect(TokenType.STRING)) {
            throw new Exception("Expected string");
        }

        String filePath = parser.peek().get().value;
        parser.consume();

        if (!parser.expect(TokenType.SEMICOLON)) {
            throw new Exception("Expected ';'");
        }
        parser.consume();

        if (importPaths.contains(filePath)) {
            return;
        }
        importPaths.add(filePath);

        parseProtocolBufferFile(filePath);
    }

    private void skipImport(Parser parser) {
        parser.consume(); // Import
        parser.consume(); // String
        parser.consume(); // Semicolon
    }

    private boolean isScalarType(Keyword keyword) {
        return keyword == Keyword.INT32
                || keyword == Keyword.STRING
                || keyword == Keyword.BOOL
                || keyword == Keyword.DOUBLE
                || keyword == Keyword.FLOAT
                || keyword == Keyword.BYTES
                || keyword == Keyword.UINT32
                || keyword == Keyword.UINT64
                || keyword == Keyword.INT64
                || keyword == Keyword.SINT32
                || keyword == Keyword.SINT64
                || keyword == Keyword.FIXED32
                || keyword == Keyword.FIXED64;
    }

    private void parseOneOf(Parser parser, MessageDefinition messageDefinition) throws Exception {
        parser.consume();

        if (!parser.expect(TokenType.IDENTIFIER)) {
            throw new Exception("Expected identifier");
        }
        String oneofIdentifier = parser.peek().get().value;
        parser.consume();

        if (!parser.expect(TokenType.OPEN_BRACE)) {
            throw new Exception("Expected '{'");
        }
        parser.consume();

        while (!parser.expect(TokenType.CLOSE_BRACE)) {
            // Checking for message fields
            MessageField field = new MessageField();

            Token token = parser.peek().get();
            if (token.type == TokenType.KEYWORD) {
                if (isScalarType(token.keyword)) {
                    field.type = token.value;
                } else {
                    throw new Exception("Expected type");
                }
            } else {
                field.type = token.value;
            }
            parser.consume();

            if (!parser.expect(TokenType.IDENTIFIER)) {
                throw new Exception("Expected identifier");
            }
            field.identifier = parser.peek().get().value;
            parser.consume();

            if (!parser.expect(TokenType.EQUAL)) {
                throw new Exception("Expected equal");
            }
            parser.consume();

            if (!parser.expect(TokenType.NUMBER)) {
                throw new Exception("Expected field number");
            }
            Integer fieldNumber = Integer.parseInt(parser.peek().get().value);
            parser.consume();

            if (!parser.expect(TokenType.SEMICOLON)) {
                throw new Exception("Expected semicolon");
            }
            parser.consume();

            messageDefinition.fields.put(fieldNumber, field);
            messageDefinition.oneofs.put(fieldNumber, oneofIdentifier);
        }
        parser.consume();
    }

    private void parseMapField(Parser parser, MessageDefinition messageDefinition) throws Exception {
        parser.consume();

        if (!parser.expect(TokenType.OPEN_ANGLE_BRACKET)) {
            throw new Exception("Expected <");
        }
        parser.consume();

        if (!parser.expect(TokenType.KEYWORD)) {
            throw new Exception("Expected keyword");
        }

        Token token = parser.peek().get();
        if (token.keyword == Keyword.DOUBLE || token.keyword == Keyword.FLOAT || token.keyword == Keyword.BYTES) {
            throw new Exception("Map key must be string or integer type");
        }
        String keyType = token.value;
        parser.consume();

        if (!parser.expect(TokenType.COMMA)) {
            throw new Exception("Expected ,");
        }
        parser.consume();

        if (!parser.expect(TokenType.KEYWORD) && !parser.expect(TokenType.IDENTIFIER)) {
            throw new Exception("Expected keyword or identifier");
        }
        String valueType = parser.peek().get().value;
        parser.consume();

        if (!parser.expect(TokenType.CLOSE_ANGLE_BRACKET)) {
            throw new Exception("Expected >");
        }
        parser.consume();

        if (!parser.expect(TokenType.KEYWORD) && !parser.expect(TokenType.IDENTIFIER)) {
            throw new Exception("Expected keyword or identifier");
        }
        String mapIdentifier = parser.peek().get().value;
        parser.consume();

        if (!parser.expect(TokenType.EQUAL)) {
            throw new Exception("Expected equal found " + parser.peek().get().value + " instead");
        }
        parser.consume();

        if (!parser.expect(TokenType.NUMBER)) {
            throw new Exception("Expected field number");
        }
        Integer fieldNumber = Integer.parseInt(parser.peek().get().value);
        parser.consume();

        if (!parser.expect(TokenType.SEMICOLON)) {
            throw new Exception("Expected semicolon");
        }
        parser.consume();

        MessageDefinition mapMessage = new MessageDefinition();
        mapMessage.identifier = messageDefinition.identifier + ".__" + mapIdentifier;
        mapMessage.fields = new HashMap<>();
        mapMessage.oneofs = new HashMap<>();

        MessageField keyField = new MessageField();
        keyField.type = keyType;
        keyField.identifier = "key";
        mapMessage.fields.put(1, keyField);

        MessageField valueField = new MessageField();
        valueField.type = valueType;
        valueField.identifier = "value";
        mapMessage.fields.put(2, valueField);

        definitions.put(mapMessage.identifier, mapMessage);

        MessageField mapField = new MessageField();
        mapField.type = mapMessage.identifier;
        mapField.identifier = mapIdentifier;
        mapField.modifier = MessageFieldModifier.MAP;

        messageDefinition.fields.put(fieldNumber, mapField);
    }

    private void parseMessageFields(Parser parser, MessageDefinition messageDefinition) throws Exception {
        if (!parser.expect(TokenType.OPEN_BRACE)) {
            throw new Exception("Expected '{'");
        }
        parser.consume();

        while (!parser.expect(TokenType.CLOSE_BRACE)) {
            // Checking for nested message definitions
            if (parser.expect(TokenType.KEYWORD)) {
                if (parser.peek().get().keyword == Keyword.MESSAGE) {
                    parseMessageDefinition(parser, messageDefinition);
                    continue;
                } else if (parser.peek().get().keyword == Keyword.ENUM) {
                    parseEnumDefinition(parser, messageDefinition);
                    continue;
                } else if (parser.peek().get().keyword == Keyword.ONEOF) {
                    parseOneOf(parser, messageDefinition);
                    continue;
                }
            }

            // Checking for message fields
            MessageField field = new MessageField();

            Token token = parser.peek().get();
            if (token.type == TokenType.KEYWORD) {
                Keyword keyword = token.keyword;
                if (keyword == Keyword.REPEATED) {
                    field.modifier = MessageFieldModifier.REPEATED;
                } else if (keyword == Keyword.OPTIONAL) {
                    field.modifier = MessageFieldModifier.OPTIONAL;
                } else if (keyword == Keyword.MAP) {
                    parseMapField(parser, messageDefinition);
                    continue;
                } else {
                    if (isScalarType(token.keyword)) {
                        field.type = token.value;
                    } else {
                        throw new Exception("Expected type found " + token.value + " instead");
                    }
                }
            } else {
                field.type = token.value;
            }
            parser.consume();

            if (field.type == null) {
                if (!parser.expect(TokenType.KEYWORD) && !parser.expect(TokenType.IDENTIFIER)) {
                    throw new Exception("Expected keyword or identifier");
                }

                token = parser.peek().get();
                if (token.type == TokenType.KEYWORD && token.keyword == Keyword.MAP) {
                    throw new Exception("Map must not have repeated or optional modifier");
                }

                field.type = token.value;

                parser.consume();
            }

            if (!parser.expect(TokenType.IDENTIFIER)) {
                throw new Exception("Expected identifier");
            }
            field.identifier = parser.peek().get().value;
            parser.consume();

            if (!parser.expect(TokenType.EQUAL)) {
                throw new Exception("Expected equal found " + parser.peek().get().value + " instead");
            }
            parser.consume();

            if (!parser.expect(TokenType.NUMBER)) {
                throw new Exception("Expected field number");
            }
            Integer fieldNumber = Integer.parseInt(parser.peek().get().value);
            parser.consume();

            if (!parser.expect(TokenType.SEMICOLON)) {
                throw new Exception("Expected semicolon");
            }
            parser.consume();

            messageDefinition.fields.put(fieldNumber, field);
        }
        parser.consume();
    }

    private void parseMessageDefinition(Parser parser, MessageDefinition parent) throws Exception {
        parser.consume();

        if (!parser.expect(TokenType.IDENTIFIER)) {
            throw new Exception("Expected identifier");
        }
        String identifier = parser.peek().get().value;
        if (parent != null) {
            identifier = parent.identifier + "." + identifier;
        }

        MessageDefinition messageDefinition = new MessageDefinition();
        messageDefinition.fields = new HashMap<>();
        messageDefinition.oneofs = new HashMap<>();
        messageDefinition.identifier = identifier;

        parser.consume();

        parseMessageFields(parser, messageDefinition);

        definitions.put(identifier, messageDefinition);
    }

    private void parseEnumDefinition(Parser parser, MessageDefinition parent) throws Exception {
        parser.consume();

        Token token = parser.peek().get();
        if (!parser.expect(TokenType.IDENTIFIER)) {
            throw new Exception("Expected identifier after " + token.value);
        }
        String identifier = parser.peek().get().value;
        if (parent != null) {
            identifier = parent.identifier + "." + identifier;
        }

        EnumDefinition protocolBufferEnum = new EnumDefinition();
        protocolBufferEnum.values = new HashMap<>();

        parser.consume();

        if (!parser.expect(TokenType.OPEN_BRACE)) {
            throw new Exception("Expected '{'");
        }
        parser.consume();

        while (!parser.expect(TokenType.CLOSE_BRACE)) {
            if (!parser.expect(TokenType.IDENTIFIER)) {
                throw new Exception("Expected identifier");
            }
            String fieldIdentifier = parser.peek().get().value;
            parser.consume();

            if (!parser.expect(TokenType.EQUAL)) {
                throw new Exception("Expected equal");
            }
            parser.consume();

            if (!parser.expect(TokenType.NUMBER)) {
                throw new Exception("Expected field number");
            }
            Integer fieldNumber = Integer.parseInt(parser.peek().get().value);
            parser.consume();

            if (!parser.expect(TokenType.SEMICOLON)) {
                throw new Exception("Expected semicolon");
            }
            parser.consume();

            protocolBufferEnum.values.put(fieldIdentifier, fieldNumber);
        }
        parser.consume();

        definitions.put(identifier, protocolBufferEnum);
    }

    private void parseServiceDefinition(Parser parser) throws Exception {
        parser.consume();

        if (!parser.expect(TokenType.IDENTIFIER)) {
            throw new Exception("Expected identifier");
        }
        String identifier = parser.peek().get().value;

        ServiceDefinition serviceDefinition = new ServiceDefinition();
        serviceDefinition.methods = new HashMap<>();

        parser.consume();

        if (!parser.expect(TokenType.OPEN_BRACE)) {
            throw new Exception("Expected '{'");
        }
        parser.consume();

        while (!parser.expect(TokenType.CLOSE_BRACE)) {
            ServiceMethod method = new ServiceMethod();

            if (!parser.expect(TokenType.KEYWORD)) {
                throw new Exception("Expected identifier");
            }
            if (parser.peek().get().keyword != Keyword.RPC) {
                throw new Exception("Expected RPC keyword");
            }
            parser.consume();

            if (!parser.expect(TokenType.IDENTIFIER)) {
                throw new Exception("Expected identifier");
            }
            String methodIdentifier = parser.peek().get().value;
            parser.consume();

            if (!parser.expect(TokenType.OPEN_PAREN)) {
                throw new Exception("Expected (");
            }
            parser.consume();

            if (!parser.expect(TokenType.IDENTIFIER)) {
                throw new Exception("Expected identifier");
            }
            method.inputIdentifier = parser.peek().get().value;
            parser.consume();

            if (!parser.expect(TokenType.CLOSE_PAREN)) {
                throw new Exception("Expected )");
            }
            parser.consume();

            if (!parser.expect(TokenType.KEYWORD)) {
                throw new Exception("Expected RETURNS keyword");
            }
            if (parser.peek().get().keyword != Keyword.RETURNS) {
                throw new Exception("Expected RETURNS keyword");
            }
            parser.consume();

            if (!parser.expect(TokenType.OPEN_PAREN)) {
                throw new Exception("Expected (");
            }
            parser.consume();

            if (!parser.expect(TokenType.IDENTIFIER)) {
                throw new Exception("Expected identifier");
            }
            method.outputIdentifier = parser.peek().get().value;
            parser.consume();

            if (!parser.expect(TokenType.CLOSE_PAREN)) {
                throw new Exception("Expected )");
            }
            parser.consume();

            if (!parser.expect(TokenType.SEMICOLON)) {
                throw new Exception("Expected semicolon");
            }
            parser.consume();

            serviceDefinition.methods.put(methodIdentifier, method);
        }
        parser.consume();

        definitions.put(identifier, serviceDefinition);
    }

    private void resolveImports(ArrayList<Token> tokens) throws Exception {
        Parser parser = new Parser(tokens);
        while (parser.peek().isPresent()) {
            Token token = parser.peek().get();
            if (token.keyword == Keyword.IMPORT) {
                parseImport(parser);
            } else {
                parser.consume();
            }
        }
    }

    private void parseProtocolBufferFile(String filePath) throws Exception {
        System.err.println("INFO: Parsing protocol buffer file: " + filePath);

        String contents = readEntireFile(filePath);
        ArrayList<Token> tokens = tokenize(contents);

        resolveImports(tokens);

        syntax = Syntax.PROTO2;
        Parser parser = new Parser(tokens);
        while (parser.peek().isPresent()) {
            Token token = parser.peek().get();

            if (token.type == TokenType.KEYWORD) {
                switch (token.keyword) {
                    case SYNTAX -> parseSyntax(parser);
                    case IMPORT -> skipImport(parser);
                    case MESSAGE -> parseMessageDefinition(parser, null);
                    case ENUM -> parseEnumDefinition(parser, null);
                    case SERVICE -> parseServiceDefinition(parser);
                    default -> throw new Exception("Parsing not implemented: " + token.keyword);
                }
            } else {
                throw new Exception("Unexpected token: " + token.type);
            }
        }

        if (syntax != Syntax.PROTO3) {
            throw new Exception("Only proto3 syntax is supported");
        }
    }
}
