package com.james.redis;

import com.james.utils.DataUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.stream.IntStream;

public class RedisDemoCmp2 {
    public static void main(String[] args) {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
        Jedis jedis = pool.getResource();

        final int KEY_CNT=1000*1000;

        int[] intLessThan4BytesArray = new int[KEY_CNT];
        int[] intMoreThan4BytesArray = new int[KEY_CNT];


        IntStream.range(0, KEY_CNT).forEachOrdered(i -> {
            intLessThan4BytesArray[i] = i % 2 + 1;
            intMoreThan4BytesArray[i] = i % 2 + 6123;

            System.out.println(intLessThan4BytesArray[i] + " vs. " + intMoreThan4BytesArray[i]);
        });

        IntStream.range(0, KEY_CNT).forEachOrdered(i -> {
            /**
             * redis-memory-for-key -s localhost -p 6379 intLessThan4BytesArray_billion_test
             * Key				intLessThan4BytesArray
             * Bytes				965
             * Type				hash
             * Encoding			ziplist
             * Number of Elements		100
             * Length of Largest Element	8
             */
            jedis.hset("intLessThan4BytesArray_billion_test", "key" + i, String.valueOf(intLessThan4BytesArray[i]));

            /**
             * redis-memory-for-key -s localhost -p 6379 intLessThan4BytesArray_bytes_billion_test
             * Key				intLessThan4BytesArray_bytes
             * Bytes				1373
             * Type				hash
             * Encoding			ziplist
             * Number of Elements		100
             * Length of Largest Element	5
             */
            jedis.hset("intLessThan4BytesArray_bytes_billion_test".getBytes(), ("key" + i).getBytes(), DataUtil.intToByteArray(intLessThan4BytesArray[i]));

            /**
             * redis-memory-for-key -s localhost -p 6379 intMoreThan4BytesArray_billion_test
             * Key				intMoreThan4BytesArray
             * Bytes				965
             * Type				hash
             * Encoding			ziplist
             * Number of Elements		100
             * Length of Largest Element	8
             */
            jedis.hset("intMoreThan4BytesArray_billion_test", "key" + i, String.valueOf(intMoreThan4BytesArray[i]));

            /**
             * redis-memory-for-key -s localhost -p 6379 intMoreThan4BytesArray_bytes_billion_test
             * Key				intMoreThan4BytesArray_bytes
             * Bytes				1373
             * Type				hash
             * Encoding			ziplist
             * Number of Elements		100
             * Length of Largest Element	5
             */
            jedis.hset("intMoreThan4BytesArray_bytes_billion_test".getBytes(), ("key" + i).getBytes(), DataUtil.intToByteArray(intMoreThan4BytesArray[i]));
        });

        String val1 = jedis.hget("intLessThan4BytesArray_billion_test", "key9");
        byte[] val2 = jedis.hget("intLessThan4BytesArray_bytes_billion_test".getBytes(), "key9".getBytes());
        System.out.println(val1 + " vs. " + val2);

//        /**
//         * create by James on 2020-05-07.
//         *
//         * 删除key
//         */
//        jedis.del("k1");

//        IntStream.range(0, 100).forEachOrdered(i -> {
//            System.out.println(i);
//            jedis.del("k1", "key" + i, "value" + i);
//        });
    }
}
