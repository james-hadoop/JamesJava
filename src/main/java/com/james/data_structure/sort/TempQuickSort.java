package com.james.data_structure.sort;

public class TempQuickSort {
    public static void main(String[] args) {
        int[] data = {21, 30, 49, 30, 21, 16, 9};
        System.out.println("排序之前：\n\t" + java.util.Arrays.toString(data));

        System.out.println();
        quickSort(data);

        System.out.println();
        System.out.println("排序之后：\n\t" + java.util.Arrays.toString(data));
    }

    public static void quickSort(int[] data) {
        subSort(data, 0, data.length - 1);
    }

    private static void subSort(int[] data, int start, int end) {
        if (start < end) {
            int base = data[start];
            int i = start;
            int j = end + 1;

            while (true) {
                while (i <= end && data[++i] < base) {
                    ;
                }
                while (j >= start && data[--j] > base) {
                    ;
                }
                if (i < j) {
                    swap(data, i, j);
                } else {
                    break;
                }
            }

            swap(data, start, j);

            subSort(data, start, j - 1);

            subSort(data, j + 1, end);

        }
    }

    private static void swap(int[] data, int i, int j) {
        int tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }
}
