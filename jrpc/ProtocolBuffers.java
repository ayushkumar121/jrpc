package jrpc;

import java.io.*;
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

    public Map<String, Definition> getDefinitions() {
        return definitions;
    }

    public enum Syntax {
        PROTO2,
        PROTO3,
    }

    public enum DefinitionType {
        MESSAGE,
        ENUM,
        SERVICE,
    }

    public interface Definition {
        DefinitionType getType();
    }

    public enum MessageFieldModifier {
        REPEATED,
        OPTIONAL,
        MAP,
    }

    public static class MessageField {
        public String identifier;
        public String type;
        public MessageFieldModifier modifier;
    }

    public static class MessageDefinition implements Definition {
        public String identifier;
        public Map<Integer, MessageField> fields;
        public Map<Integer, String> oneofs;

        @Override
        public DefinitionType getType() {
            return DefinitionType.MESSAGE;
        }
    }

    public static class EnumDefinition implements Definition {
        public String identifier;
        public Map<String, Integer> values;

        @Override
        public DefinitionType getType() {
            return DefinitionType.ENUM;
        }
    }

    public static class ServiceMethodDefinition {
        public String inputIdentifier;
        public String outputIdentifier;
    }

    public static class ServiceDefinition implements Definition {
        public String identifier;
        public Map<String, ServiceMethodDefinition> methods;

        @Override
        public DefinitionType getType() {
            return DefinitionType.SERVICE;
        }
    }

    // Private methods
    
    private Syntax syntax;
    private final List<String> importPaths;
    private final Map<String, Definition> definitions;

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

        public final String value;

        Keyword(String value) {
            this.value = value;
        }
    }

    private static class Token {
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

    private static class Parser {
        private final ArrayList<Token> tokens;
        private int index;

        public Parser(ArrayList<Token> tokens) {
            this.tokens = tokens;
            this.index = 0;
        }

        public boolean expect(TokenType type) {
            return index < tokens.size() && tokens.get(index).type == type;
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
        protocolBufferEnum.identifier = identifier;
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
        serviceDefinition.identifier = identifier;
        serviceDefinition.methods = new HashMap<>();

        parser.consume();

        if (!parser.expect(TokenType.OPEN_BRACE)) {
            throw new Exception("Expected '{'");
        }
        parser.consume();

        while (!parser.expect(TokenType.CLOSE_BRACE)) {
            ServiceMethodDefinition method = new ServiceMethodDefinition();

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
