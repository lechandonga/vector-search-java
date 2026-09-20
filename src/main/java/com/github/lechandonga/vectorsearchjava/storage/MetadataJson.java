package com.github.lechandonga.vectorsearchjava.storage;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 极简 JSON 编解码器，只支持 {@code Map<String,String>}。
 *
 * <p>不引入第三方依赖；序列化结果确定（按迭代顺序），解析严格：
 * 任何多余空白之外的语法、非法转义、重复键都会抛出 {@link WalCorruptionException}。
 */
final class MetadataJson {

    private MetadataJson() {
    }

    static String write(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (e.getKey() == null || e.getValue() == null) {
                throw new IllegalArgumentException("metadata keys and values must be non-null");
            }
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(quote(e.getKey())).append(':').append(quote(e.getValue()));
        }
        return sb.append('}').toString();
    }

    static Map<String, String> read(String json) throws WalCorruptionException {
        if (json == null) {
            return Map.of();
        }
        Parser p = new Parser(json);
        p.skipWs();
        Map<String, String> result = p.parseObject();
        p.skipWs();
        if (!p.eof()) {
            throw new WalCorruptionException("trailing characters after JSON object");
        }
        return result;
    }

    private static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) {
            this.s = s;
        }

        boolean eof() {
            return pos >= s.length();
        }

        void skipWs() {
            while (!eof() && Character.isWhitespace(s.charAt(pos))) {
                pos++;
            }
        }

        char peek() throws WalCorruptionException {
            if (eof()) {
                throw new WalCorruptionException("unexpected end of JSON");
            }
            return s.charAt(pos);
        }

        void expect(char c) throws WalCorruptionException {
            if (eof() || s.charAt(pos) != c) {
                throw new WalCorruptionException("expected '" + c + "' at position " + pos);
            }
            pos++;
        }

        Map<String, String> parseObject() throws WalCorruptionException {
            expect('{');
            Map<String, String> map = new LinkedHashMap<>();
            skipWs();
            if (peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWs();
                String key = parseString();
                skipWs();
                expect(':');
                skipWs();
                String value = parseString();
                if (map.put(key, value) != null) {
                    throw new WalCorruptionException("duplicate metadata key: " + key);
                }
                skipWs();
                char c = s.charAt(pos++);
                if (c == '}') {
                    return map;
                }
                if (c != ',') {
                    throw new WalCorruptionException("expected ',' or '}' at position " + (pos - 1));
                }
            }
        }

        String parseString() throws WalCorruptionException {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (eof()) {
                    throw new WalCorruptionException("unterminated string");
                }
                char c = s.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (eof()) {
                        throw new WalCorruptionException("unterminated escape");
                    }
                    char e = s.charAt(pos++);
                    switch (e) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'u' -> {
                            if (pos + 4 > s.length()) {
                                throw new WalCorruptionException("invalid \\u escape");
                            }
                            String hex = s.substring(pos, pos + 4);
                            try {
                                sb.append((char) Integer.parseInt(hex, 16));
                            } catch (NumberFormatException nfe) {
                                throw new WalCorruptionException("invalid \\u escape: " + hex);
                            }
                            pos += 4;
                        }
                        default -> throw new WalCorruptionException("invalid escape: \\" + e);
                    }
                } else if (c < 0x20) {
                    throw new WalCorruptionException("unescaped control character in string");
                } else {
                    sb.append(c);
                }
            }
        }
    }
}
