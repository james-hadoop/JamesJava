package com.james.class_loader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class JamesClassLoader extends ClassLoader {
    public static void main(String[] args)
            throws Exception {
        JamesClassLoader cl = new JamesClassLoader(Thread.currentThread().getContextClassLoader(),
                "/Users/qjiang/workspace/JamesJava/src/main/java/com/james/class_loader");

        Class<?> clazz = cl.loadClass("com.james.class_loader.Animal");

        Animal animal = (Animal) clazz.newInstance();
        animal.say();
    }

    private String path = "/Users/qjiang/workspace/JamesJava/src/main/java/com/james/class_loader";

    public JamesClassLoader(String path) {
        this.path = path;
    }

    public JamesClassLoader(ClassLoader parent, String path) {
        super(parent);
        this.path = path;
    }

    @Override
    public Class<?> findClass(String name) {
        byte[] data = loadClassData(name);
        return defineClass(name, data, 0, data.length);
    }

    private byte[] loadClassData(String name) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File(path + name + ".class"));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int b;
            while ((b = fis.read()) != -1) {
                baos.write(b);
            }
            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != fis) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return null;
    }
}
