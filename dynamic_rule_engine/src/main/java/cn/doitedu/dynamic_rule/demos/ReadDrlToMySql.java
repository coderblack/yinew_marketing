package cn.doitedu.dynamic_rule.demos;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-04-07
 * @desc
 *
 *   `id` int(11) NOT NULL AUTO_INCREMENT,
 *   `rule_name` varchar(255) DEFAULT NULL,
 *   `rule_code` varchar(4096) DEFAULT NULL,
 *   `rule_status` int(11) DEFAULT NULL,
 *   `rule_type` varchar(255) DEFAULT NULL,
 *   `rule_version` varchar(255) DEFAULT NULL,
 *   `cnt_sqls` varchar(4096) DEFAULT NULL,
 *   `seq_sqls` varchar(4096) DEFAULT NULL,
 *   `rule_creator` varchar(255) DEFAULT NULL,
 *   `rule_auditor` varchar(255) DEFAULT NULL,
 *   `create_time` datetime DEFAULT NULL,
 *   `update_time` datetime DEFAULT NULL,
 *
 */
public class ReadDrlToMySql {
    public static void main(String[] args) throws IOException, SQLException {
        String ruleName = "rule2";
        String ruleCode = FileUtils.readFileToString(new File("dynamic_rule_engine/rules_drl/rule2.drl"), "utf-8");
        int ruleStatus = 1;
        String ruleType = "1";
        String ruleVersion = "1";
        String cntSqls = FileUtils.readFileToString(new File("dynamic_rule_engine/rules_drl/rule2_cnt.sql"), "utf-8");
        String seqSqls = FileUtils.readFileToString(new File("dynamic_rule_engine/rules_drl/rule2_seq.sql"), "utf-8");
        String ruleCreator = "doitedu";
        String ruleAuditor = "hunter.d";
        Date createTime = new Date(System.currentTimeMillis());
        Date updateTime = createTime;
        Connection conn = DriverManager.getConnection("jdbc:mysql://hdp01:3306/realtimedw", "root", "ABC123abc.123");
        PreparedStatement pst = conn.prepareStatement("insert into yinew_drl_rule (rule_name,rule_code,rule_status,rule_type,rule_version,cnt_sqls,seq_sqls,rule_creator,rule_auditor,create_time,update_time) " +
                "values (?,?,?,?,?,?,?,?,?,?,?)");
        pst.setString(1,ruleName);
        pst.setString(2,ruleCode);
        pst.setInt(3,ruleStatus);
        pst.setString(4,ruleType);
        pst.setString(5,ruleVersion);
        pst.setString(6,cntSqls);
        pst.setString(7,seqSqls);
        pst.setString(8,ruleCreator);
        pst.setString(9,ruleAuditor);
        pst.setDate(10,createTime);
        pst.setDate(11,updateTime);

        boolean execute = pst.execute();

        pst.close();
        conn.close();


    }
}
