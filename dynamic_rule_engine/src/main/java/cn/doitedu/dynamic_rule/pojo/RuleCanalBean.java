package cn.doitedu.dynamic_rule.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleCanalBean {

    private List<RuleTableRecord> data;
    private String type;

}
