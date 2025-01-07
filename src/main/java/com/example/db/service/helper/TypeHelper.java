package com.example.db.service.helper;

public class TypeHelper {
    public static boolean isInteger(String value) {
        try {
            Integer.parseInt(value);
            return true;
        }
        catch (Exception ex) {
            return false;
        }
    }

    public static Integer toInt(String value) {
        return Integer.parseInt(value);
    }
}
