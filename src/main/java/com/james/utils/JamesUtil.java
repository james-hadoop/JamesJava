package com.james.utils;

import com.james.data_structure.tree.BinTree;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JamesUtil {
    public static void printDivider() {
        System.out.println("\n---------------------------------- divider ----------------------------------\n\n");
    }

    public static void printDivider(String divider) {
        if (null == divider) {
            printDivider();
        }

        System.out.println("\n---------------------------------- " + divider + " ----------------------------------\n\n");
    }

    public static void printCollection(Collection<String> collection) {
        if (null == collection || 0 == collection.size()) {
            return;
        }

        for (String c : collection) {
            System.out.println(c);
        }
    }

    public static void printArray(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i]);
            System.out.print("\t");
        }
        System.out.println();
    }

    public static void printArray(String[] arr) {
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i]);
            System.out.print("\t");
        }
        System.out.println();
    }

    public static void printTreeList(List<BinTree.TreeNode> list, String mode) {
        System.out.println(mode + " tree: ");
        for (BinTree.TreeNode node : list) {
            System.out.print(node.data + "\t");
        }
        System.out.println("\n");
    }

    public static void printMap(Map<String, Integer> map) {
        if (null == map || 0 == map.size()) {
            return;
        }

        Set<String> setKey = map.keySet();
        for (String key : setKey) {
            System.out.println("key: " + key + " --> " + "value: " + map.get(key));
        }
        System.out.println();
    }
}
