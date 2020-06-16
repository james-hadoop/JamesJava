package com.james._demo;

import org.apache.commons.codec.binary.Base64;

public class Base64Demo {
    public static void main(String[] args) {
        String str = "Hello World!";
        System.out.println(str);

        byte[] input = str.getBytes();
        System.out.println(input);

        String encode = Base64.encodeBase64String(input);
        System.out.println(encode);

        byte[] bytes = Base64.decodeBase64(encode);
        String output = new String(bytes);
        System.out.println(output);
    }
}
