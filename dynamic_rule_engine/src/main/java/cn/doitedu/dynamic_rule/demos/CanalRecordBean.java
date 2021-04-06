package cn.doitedu.dynamic_rule.demos;

import lombok.Data;

import java.util.List;

@Data
public class CanalRecordBean {

    private List<RuleTableRecord> data;
    private String type;

}


@Data
class RuleTableRecord{

    private String ruleName;
    private String ruleCode;


}