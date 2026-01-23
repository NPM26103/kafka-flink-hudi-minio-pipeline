package com.example.common;

import java.util.HashMap;
import java.util.Map;

public class Args {
    private final Map<String, String> m = new HashMap<>();

    public static Args parse(String[] args) {
        Args a = new Args();
        if (args == null) return a;

        for (int i = 0; i < args.length; i++) {
            String tok = args[i];
            if (tok == null) continue;
            tok = tok.trim();
            if (tok.isEmpty()) continue;
            if (!tok.startsWith("--")) continue;

            // --key=value
            if (tok.contains("=")) {
                String[] kv = tok.substring(2).split("=", 2);
                a.m.put(kv[0].trim(), kv.length > 1 ? kv[1].trim() : "");
                continue;
            }

            // --key value
            String key = tok.substring(2).trim();
            String val = "";
            if (i + 1 < args.length) {
                String next = args[i + 1];
                if (next != null) {
                    next = next.trim();
                    if (!next.startsWith("--")) {
                        val = next;
                        i++;
                    }
                }
            }
            a.m.put(key, val);
        }
        return a;
    }

    public String get(String k, String def) {
        // 1) args
        String v = m.get(k);
        if (v != null && !v.isBlank()) return v.trim();

        // 2) env fallback: thử cả UPPER_SNAKE và nguyên key
        String env1 = System.getenv(toEnvKey(k));
        if (env1 != null && !env1.isBlank()) return env1.trim();

        String env2 = System.getenv(k);
        if (env2 != null && !env2.isBlank()) return env2.trim();

        return def;
    }

    public String must(String k) {
        String v = get(k, null);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing --" + k);
        return v;
    }

    public long getLong(String k, long def) {
        String v = get(k, null);
        if (v == null || v.isBlank()) return def;
        return Long.parseLong(v.trim());
    }

    public int getInt(String k, int def) {
        String v = get(k, null);
        if (v == null || v.isBlank()) return def;
        return Integer.parseInt(v.trim());
    }

    public boolean getBool(String k, boolean def) {
        String v = get(k, null);
        if (v == null || v.isBlank()) return def;
        v = v.trim().toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("y") || v.equals("on");
    }

    private String toEnvKey(String k) {
        return k.replace('.', '_').replace('-', '_').toUpperCase();
    }

    @Override
    public String toString() {
        return m.toString();
    }
}
