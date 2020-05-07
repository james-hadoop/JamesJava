package com.james.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.stream.IntStream;

public class RedisDemo {
    public static void main(String[] args) {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
        Jedis jedis = pool.getResource();

//        /**
//         * redis-memory-for-key -s localhost -p 6379 k1
//         * Key				k1
//         * Bytes				73
//         * Type				hash
//         * Encoding			ziplist
//         * Number of Elements		1
//         * Length of Largest Element	6
//         */
//        jedis.hset("k1", "key1", "value1");
        jedis.del("k1");

        IntStream.range(0, 100).forEachOrdered(i -> {
            System.out.println(i);
            jedis.del("k1", "key" + i, "value" + i);
        });

//        /**
//         * redis-memory-for-key -s localhost -p 6379 key1
//         * Key				key1
//         * Bytes				56
//         * Type				string
//         */
//        jedis.set("key1", "value1");


//        /**
//         * redis-memory-for-key -s localhost -p 6379 k1
//         * Key				k1
//         * Bytes		    1639
//         * Type				hash
//         * Encoding			ziplist
//         * Number of Elements		100
//         * Length of Largest Element	7
//         */
//        IntStream.range(0, 100).forEachOrdered(i -> {
//            System.out.println(i);
//            jedis.hset("k1", "key" + i, "value" + i);
//        });
    }
}
