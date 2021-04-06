package cn.doitedu.dynamic_rule.demos;

import lombok.Data;

/**
 * 申请人，用作fact
 */
@Data
public class Applicant {
    private String name;
    private int age;
    private boolean valid;
    public Applicant(String name, int age) {
        this.name = name;
        this.age = age;
        this.valid = true;
    }
}