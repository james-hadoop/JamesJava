package com.james.james_code;

public class TempPasswdStrengthQualifiedAssessor {
    private static final int PASSWD_STRENGTH_LEVEL = 3;

    public static void main(String[] args) {
        // false
        System.out.println(isPasswdStrengthQualified("HelloWorld"));
        // true
        System.out.println(isPasswdStrengthQualified("HelloWorld12"));
        // true
        System.out.println(isPasswdStrengthQualified("HelloWorld12]"));
    }

    public static boolean isPasswdStrengthQualified(String passwd) {
        if (passwd == null || passwd.length() < 3) {
            return false;
        }

        int passwdLevel = calPasswdLevel(passwd);
        if (passwdLevel > 3) {
            return true;
        }

        return false;
    }

    private static int calPasswdLevel(String passwd) {
        int passwdValue = 0;

        for (int i = 0; i < passwd.length(); i++) {
            char ch = passwd.charAt(i);
            passwdValue = passwdValue | passwdType(ch);
        }

        int passwdLevel = 0;
        for (int i = 0; i < Integer.SIZE; i++) {
            passwdLevel += passwdValue & 1;
            passwdValue = passwdValue >> 1;
        }

        return passwdLevel;
    }

    private static int passwdType(char c) {
        if (c >= 97 && c <= 122) {
            return 1;
        }
        if (c >= 65 && c <= 90) {
            return 2;
        }
        if (c >= 48 && c <= 57) {
            return 4;
        }
        return 8;
    }
}
