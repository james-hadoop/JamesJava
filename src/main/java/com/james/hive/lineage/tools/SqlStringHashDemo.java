package com.james.hive.lineage.tools;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Created by James on 20-7-25 下午9:59
 */
public class SqlStringHashDemo {
    public static void main(String[] args) {
        final String plainStr = "hello james";
        System.out.println("byte1's sha1Hex: " + DigestUtils.sha1Hex(plainStr.getBytes()));
    }
}
