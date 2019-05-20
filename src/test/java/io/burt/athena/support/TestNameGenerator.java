package io.burt.athena.support;

import org.junit.jupiter.api.DisplayNameGenerator;

import java.lang.reflect.Method;

public class TestNameGenerator implements DisplayNameGenerator {
    @Override
    public String generateDisplayNameForClass(Class<?> testClass) {
        return testClass.getSimpleName();
    }

    @Override
    public String generateDisplayNameForNestedClass(Class<?> nestedClass) {
        if (isFirstLevelNestedClass(nestedClass)) {
            return "#" + lowerCaseFirst(nestedClass.getSimpleName());
        } else {
            return camelCaseToWords(nestedClass.getSimpleName());
        }
    }

    private boolean isFirstLevelNestedClass(Class<?> nestedClass) {
        return !nestedClass.getEnclosingClass().getName().contains("$");
    }

    private String lowerCaseFirst(String str) {
        return str.substring(0, 1).toLowerCase() + str.substring(1);
    }

    @Override
    public String generateDisplayNameForMethod(Class<?> testClass, Method testMethod) {
        return camelCaseToWords(testMethod.getName());
    }

    private String camelCaseToWords(String str) {
        return str.replaceAll("(.)(?=\\p{Upper}|\\d)", "$1 ").toLowerCase();
    }
}
