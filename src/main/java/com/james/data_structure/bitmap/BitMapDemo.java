package com.james.data_structure.bitmap;

import com.james.utils.DataUtil;

public class BitMapDemo {
    public static void main(String[] args) {
        System.out.println("BitMapDemo start...");

//        String strCity = "220700";
//        int intCity = 220700;
//
//        byte[] strCityBytes = strCity.getBytes();
//        byte[] intCityBytes = DataUtil.intToByteArray(intCity);
//
//        System.out.println(strCityBytes.length);
//        for (int i = 0; i < strCityBytes.length; i++) {
//            System.out.print(strCityBytes[i] + "\t");
//        }
//        JamesUtil.printDivider();
//
//        System.out.println(intCityBytes.length);
//        for (int i = 0; i < intCityBytes.length; i++) {
//            System.out.print(intCityBytes[i] + "\t");
//        }
//        System.out.println("\n" + DataUtil.byteArrayToInt(intCityBytes));

        int intAge=199;
        byte[] intAgeBytes = DataUtil.intToByteArray(intAge);
        System.out.println(intAgeBytes.length);
        for (int i = 0; i < intAgeBytes.length; i++) {
            System.out.print(intAgeBytes[i] + "\t");
        }
        System.out.println("\n" + DataUtil.byteArrayToInt(intAgeBytes));
    }


}
