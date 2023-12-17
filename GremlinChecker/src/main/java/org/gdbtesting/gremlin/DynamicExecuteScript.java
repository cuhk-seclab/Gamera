package org.gdbtesting.gremlin;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.codehaus.groovy.runtime.InvokerHelper;

import java.security.MessageDigest;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class DynamicExecuteScript {

    private static ConcurrentHashMap<String, Class<Script>> zlassMaps
            = new ConcurrentHashMap<>();

    public static Object invoke(String scriptText, Map<String, Object> params) {
        String key = fingerKey(scriptText);
        Class<Script> script = zlassMaps.get(key);
        if (script == null) {
            synchronized (key.intern()) {
                // Double Check
                script = zlassMaps.get(key);
                if (script == null) {
                    GroovyClassLoader classLoader = new GroovyClassLoader();
                    script = classLoader.parseClass(scriptText);
                    zlassMaps.put(key, script);
                }
            }
        }

        Binding binding = new Binding();
        for (Map.Entry<String, Object> ent : params.entrySet()) {
            binding.setVariable(ent.getKey(), ent.getValue());
        }
        Script scriptObj = InvokerHelper.createScript(script, binding);
        return scriptObj.run();

    }

    public static String fingerKey(String scriptText) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(scriptText.getBytes("utf-8"));

            final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();
            StringBuilder ret = new StringBuilder(bytes.length * 2);
            for (int i = 0; i < bytes.length; i++) {
                ret.append(HEX_DIGITS[(bytes[i] >> 4) & 0x0f]);
                ret.append(HEX_DIGITS[bytes[i] & 0x0f]);
            }
            return ret.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        String script = "println 'hello ' + name ;";
        Map<String, Object> params = new TreeMap<String, Object>();
        params.put("name", "lilei");
        invoke(script, params);
    }

}
