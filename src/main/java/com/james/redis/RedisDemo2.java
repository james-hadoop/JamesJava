package com.james.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.stream.IntStream;

public class RedisDemo2 {

    private static int[] intMoreThan4BytesArray;

    public static void main(String[] args) {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
        Jedis jedis = pool.getResource();

        final int KEY_CNT = 1000 * 100;
        intMoreThan4BytesArray = new int[KEY_CNT];

        IntStream.range(0, KEY_CNT).forEachOrdered(i -> {
            intMoreThan4BytesArray[i] = 6123 + 1;
        });

        IntStream.range(0, KEY_CNT).forEachOrdered(i -> {
//            /**
//             * ls -lt
//             * total 8200
//             * -rw-r--r--  1 qjiang  staff  4087913  5  8 10:05 appendonly.aof
//             *
//             * redis-memory-for-key -s localhost -p 6379 strKey99999
//             * Key                             strKey99999
//             * Bytes                           56
//             * Type                            string
//             */
//            jedis.set("strKey" + i, String.valueOf(intMoreThan4BytesArray[i]));

            /**
             * ls -lt
             * total 20488
             * -rw-r--r--  1 qjiang  staff  9475803  5  8 10:09 appendonly.aof
             *
             * redis-memory-for-key -s localhost -p 6379 setKey
             * Key                             setKey
             * Bytes                           5573004.0
             * Type                            hash
             * Encoding                        hashtable
             * Number of Elements              100000
             * Length of Largest Element       11
             */
            jedis.hset("setKey", "strKey" + i, String.valueOf(intMoreThan4BytesArray[i]));
        });

//        /**
//         * create by James on 2020-05-07.
//         *
//         * 删除key
//         */
//        jedis.del("k1");
//
//        IntStream.range(0, 100).forEachOrdered(i -> {
//            System.out.println(i);
//            jedis.del("k1", "key" + i, "value" + i);
//        });
    }
}
