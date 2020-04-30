package com.james.class_loader;

import com.james.class_loader.JamesClassLoader2.Cat;

public class ClassLoaderDriver {
    public static void main(String[] args)
            throws Exception {
        JamesClassLoader cl = new JamesClassLoader(Thread.currentThread().getContextClassLoader(),
                "/Users/qjiang/workspace/JamesJava/src/main/java/com/james/class_loader");

        Class<?> clazz = cl.loadClass("com.james.class_loader.JamesClassLoader2$Cat");

        Cat cat = (Cat) clazz.getConstructors()[0].newInstance(new JamesClassLoader2());
        cat.say();
    }
}
