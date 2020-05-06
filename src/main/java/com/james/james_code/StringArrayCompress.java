package com.james.james_code;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class StringArrayCompress {
    public static void main(String[] args) throws Exception {
        String[] arr = {"Shanghai", "Beijing", "Shanghai", "Shenzhen", "Beijing"};

        Map<String, Integer> map = new HashMap<>();

        for (String k : arr) {
            if (map.keySet().contains(k)) {
                map.put(k, map.get(k) + 1);
            } else {
                map.put(k, 1);
            }
        }

        String rawFileName = "data/string_array_compress2.txt";
        String gzFileName = "data/string_array_compress2.txt.gz";

        File file = new File(rawFileName);
        file.createNewFile();
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
        oos.writeObject(map);

        oos.close();

        compress(rawFileName);

        decompress(gzFileName);

        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
        Map<String, Integer> readMap = (Map<String, Integer>) ois.readObject();

        for (Map.Entry<String, Integer> entry : readMap.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                System.out.println(entry.getKey());
            }
        }

        ois.close();
    }

    public static void compress(String path) throws Exception {
        compress(path, true);
    }

    public static void compress(String path, boolean delete) throws Exception {
        File file = new File(path);

        if (!file.isDirectory()) {
            compress(file, delete);
        }

        File[] files = file.listFiles();
        {
            if (null == files || 1 > files.length) {
                return;
            }

            for (File f : files) {
                compress(f, true);
            }
        }
    }

    public static void compress(InputStream is, OutputStream os) throws Exception {

        GZIPOutputStream gos = new GZIPOutputStream(os);

        int count;
        byte data[] = new byte[1024];
        while ((count = is.read(data, 0, 1024)) != -1) {
            gos.write(data, 0, count);
        }

        gos.finish();

        gos.flush();
        gos.close();
    }

    public static void compress(File file, boolean delete) throws Exception {
        FileInputStream fis = new FileInputStream(file);
        FileOutputStream fos = new FileOutputStream(file.getPath() + ".gz");

        compress(fis, fos);

        fis.close();
        fos.flush();
        fos.close();

        if (delete) {
            file.delete();
        }
    }

    public static void compress(File file) throws Exception {
        if (!file.isDirectory()) {
            compress(file, true);
        }

        File[] files = file.listFiles();
        {
            if (null == files || 1 > files.length) {
                return;
            }

            for (File f : files) {
                compress(f, true);
            }
        }
    }

    public static void decompress(String path) throws Exception {
        decompress(path, true);
    }

    public static void decompress(String path, boolean delete) throws Exception {
        File file = new File(path);
        decompress(file, delete);
    }

    public static void decompress(File file, boolean delete) throws Exception {
        FileInputStream fis = new FileInputStream(file);
        FileOutputStream fos = new FileOutputStream(file.getPath().replace(".gz", ""));
        decompress(fis, fos);
        fis.close();
        fos.flush();
        fos.close();

        if (delete) {
            file.delete();
        }
    }

    public static void decompress(InputStream is, OutputStream os) throws Exception {
        GZIPInputStream gis = new GZIPInputStream(is);

        int count;
        byte data[] = new byte[1024];
        while ((count = gis.read(data, 0, 1024)) != -1) {
            os.write(data, 0, count);
        }

        gis.close();
    }
}
