package cn.doitedu.dynamic_rule.demos;

import lombok.Data;

@Data
public class Action {
    private String msg;
    public Action(String msg) {
        this.msg = msg;
    }
    public void doSomeThing() {
        System.out.println(msg);
    }
}
