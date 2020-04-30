package com.james.encryption;

import com.james.utils.DataUtil;

public class Driver {

    public static void main(String[] args) {
        String world = "root123";

        String encode = DataUtil.getMD5Str(world).toLowerCase();

        System.out.println("encode: " + encode);
    }
}
