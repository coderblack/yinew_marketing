package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.utils.ConnectionUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

@Slf4j
public class UserActionCountQueryServiceClickhouseImpl implements UserActionCountQueryService {

    private static Connection conn;

    static {
        // 获取clickhouse的jdbc连接对象
        try {
            conn = ConnectionUtils.getClickhouseConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     *
     * 根据给定的deviceId，查询这个人是否满足ruleParam中的所有“次数类"规则条件
     * @param deviceId   要查询的用户
     * @param ruleParam  规则参数对象
     * @return 条件查询的结果是否成立
     * @throws Exception
     */
    @Override
    public boolean queryActionCounts(String deviceId,  RuleParam ruleParam) throws Exception {

        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();

        // 遍历每一个原子条件进行查询判断
        for (RuleAtomicParam atomicParam : userActionCountParams) {
            queryActionCounts(deviceId,atomicParam,ruleParam.getRuleName());
        }
        // 如果走到这一句代码，说明上面的每一个原子条件查询后都满足规则，那么返回最终结果true
        return true;
    }

    /**
     *
     * 查询单个行为count条件是否成立
     * @param deviceId 设备id
     * @param atomicParam 原子条件
     * @return 是否成立
     * @throws Exception 异常
     */
    @Override
    public boolean queryActionCounts(String deviceId, RuleAtomicParam atomicParam,String ruleId) throws Exception {
        // 对当前的原子条件拼接查询sql
        String sql = atomicParam.getCountQuerySql();
        // 获取一个clickhouse 的jdbc连接
        PreparedStatement ps = conn.prepareStatement(sql);
        // 需要将sql中的deviceId占位符替换成真实deviceId
        ps.setString(1,deviceId);
        ps.setLong(2,atomicParam.getRangeStart());
        ps.setLong(3,atomicParam.getRangeEnd());

        ResultSet resultSet = ps.executeQuery();

        /*sql = sql.replaceAll("\\$\\{did\\}",deviceId);
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);*/

        /* *
         * deviceId,cnt
         *  000001 ,6
         **/
        // resultSet只有一行，所以while只循环一次
        while (resultSet.next()) {
            // 从结果中取出cnt字段
            int realCnt = (int) resultSet.getLong(2);
            // 将查询结果赛回规则参数对象
            log.info("规则:{},用户:{},查询clickhouse次数条件,查询前cnt:{},本次cnt:{},累加后cnt:{}",ruleId,deviceId,atomicParam.getRealCnt(),realCnt,atomicParam.getRealCnt()+realCnt);
            atomicParam.setRealCnt(atomicParam.getRealCnt() + realCnt);
        }

        // 只要有一个原子条件查询结果不满足，则直接返回最终结果false
        if (atomicParam.getRealCnt() < atomicParam.getCnt()) {
            return false;
        } else {
            return true;
        }
    }
}
