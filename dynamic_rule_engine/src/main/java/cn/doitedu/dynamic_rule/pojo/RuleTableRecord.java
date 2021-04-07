package cn.doitedu.dynamic_rule.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleTableRecord {

    private int id;
    private String rule_name;
    private String rule_code;
    private int rule_status;
    private String rule_type;
    private String rule_versioin;
    private String cnt_sqls;
    private String seq_sqls;

}
