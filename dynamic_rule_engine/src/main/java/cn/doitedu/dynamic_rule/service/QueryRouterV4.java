package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.BufferResult;
import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.utils.RuleCalcUtil;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.state.ListState;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-31
 * @desc 查询路由模块（加入缓存后的版本）
 */
public class QueryRouterV4 {

    // 画像查询服务
    private UserProfileQueryService userProfileQueryService;

    // count类条件state查询服务
    private UserActionCountQueryService userActionCountQueryStateService;

    // sequence类条件state查询服务
    private UserActionSequenceQueryService userActionSequenceQueryStateService;

    // count类条件clickhouse查询服务
    private UserActionCountQueryService userActionCountQueryClickhouseService;

    // sequence类条件clickhouse查询服务
    private UserActionSequenceQueryService userActionSequenceQueryClickhouseService;

    // 缓存管理器
    BufferManager bufferManager;

    ListState<LogBean> eventState;

    public QueryRouterV4(ListState<LogBean> eventState) throws Exception {

        userProfileQueryService = new UserProfileQueryServiceHbaseImpl();

        /**
         * 构造STATE查询服务
         */
        userActionCountQueryStateService = new UserActionCountQueryServiceStateImpl();
        userActionSequenceQueryStateService = new UserActionSequenceQueryServiceStateImpl();

        /**
         * 构造CLICKHOUSE查询服务
         */
        userActionCountQueryClickhouseService = new UserActionCountQueryServiceClickhouseImpl();
        userActionSequenceQueryClickhouseService = new UserActionSequenceQueryServiceClickhouseImpl();

        /**
         * 构造一个缓存管理器
         */
        bufferManager = new BufferManager();

        this.eventState = eventState;

    }

    // 控制画像条件查询路由
    public boolean profileQuery(LogBean logBean, RuleParam ruleParam) {

        /* **
         * TODO 从缓存中获取结果（如果hbase集群能力强，可不将画像查询结果放入缓存）
         *******/


        // 从hbase中查询
        boolean profileIfMatch = userProfileQueryService.judgeProfileCondition(logBean.getDeviceId(), ruleParam);

        /* **
         * TODO 将查询结果插入缓存（如果hbase集群能力强，可不将画像查询结果放入缓存）
         *******/
        if (!profileIfMatch) return false;
        return true;
    }


    /**
     * 控制count类条件查询路由
     * @param logBean   待计算事件
     * @param ruleParam 规则参数对象
     * @return 是否匹配
     * @throws Exception 异常
     */
    public boolean countConditionQuery(LogBean logBean, RuleParam ruleParam) throws Exception {

        // 取出规则中的count类条件
        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();

        /*
         * 缓存查询处理：
         *   1.遍历规则中的各个条件，逐个条件去缓存中查询
         *   2.依据查询后的结果，如果已经完全匹配的条件，从规则中直接剔除
         *   3.如果是部分有效的，则更新条件时间起始点为缓存结束点
         ******/
        for (int i = 0; i < userActionCountParams.size(); i++) {
            // 拼接bufferKey
            String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), userActionCountParams.get(i));
            // 从redis中取数据
            BufferResult bufferResult = bufferManager.getBufferData(bufferKey, userActionCountParams.get(i).getRangeStart(), userActionCountParams.get(i).getRangeEnd(), userActionCountParams.get(i).getCnts());

            switch (bufferResult.getBufferAvailableLevel()) {
                // 如果是部分有效
                case PARTIAL_AVL:
                    // 则更新条件的窗口起始点
                    userActionCountParams.get(i).setRangeStart(bufferResult.getBufferRangeEnd());
                    // 将value值，放入参数对象的realCnt中
                    userActionCountParams.get(i).setRealCnts(bufferResult.getBufferValue());
                    break;
                // 如果是完全有效，则剔除该条件
                case WHOLE_AVL:
                    userActionCountParams.remove(i);
                    break;
                case UN_AVL:
            }

        }


        // 计算查询分界点timestamp ：当前时间对小时取整，-1
        long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(), Calendar.HOUR), -2).getTime();

        // 遍历规则中的事件count类条件，按照时间跨度，分成3组
        ArrayList<RuleAtomicParam> farRangeParams = new ArrayList<>();  // 远期条件组
        ArrayList<RuleAtomicParam> nearRangeParams = new ArrayList<>();  // 近期条件组
        ArrayList<RuleAtomicParam> crossRangeParams = new ArrayList<>();  // 近期条件组
        for (RuleAtomicParam userActionCountParam : userActionCountParams) {
            if (userActionCountParam.getRangeEnd() < splitPoint) {
                // 如果条件结end<分界点，放入远期条件组
                farRangeParams.add(userActionCountParam);
            } else if (userActionCountParam.getRangeStart() >= splitPoint) {
                // 如果条件start>=分界点，放入近期条件组
                nearRangeParams.add(userActionCountParam);
            } else {
                // 否则，就放入跨界条件组
                crossRangeParams.add(userActionCountParam);
            }
        }

        /*
         * 近期条件组，批量打包查询
         *****/
        if (nearRangeParams.size() > 0) {
            // 将规则总参数对象中的“次数类条件”覆盖成： 近期条件组
            ruleParam.setUserActionCountParams(nearRangeParams);
            // 交给stateService对这一组条件进行计算
            boolean countMatch = userActionCountQueryStateService.queryActionCounts("", eventState, ruleParam);

            /* **
             * TODO 将查询结果插入缓存
             *******/
            if (!countMatch) return false;
        }

        /*
         * 远期条件组，批量打包查询
         ****/
        if (farRangeParams.size() > 0) {
            // 将规则总参数对象中的“次数类条件”覆盖成： 远期条件组
            ruleParam.setUserActionCountParams(farRangeParams);
            boolean b = userActionCountQueryClickhouseService.queryActionCounts(logBean.getDeviceId(), null, ruleParam);

            /* **
             * TODO 将查询结果插入缓存
             *******/
            if (!b) return false;
        }


        /**
         * 跨界count条件组查询
         * 叶子条件逐一控制查询
         */
        for (RuleAtomicParam crossRangeParam : crossRangeParams) {
            long originRangeStart = crossRangeParam.getRangeStart();
            long originEnd = crossRangeParam.getRangeEnd();

            // 将参数时段替换[分界点,原end]，去state service中查询
            crossRangeParam.setRangeStart(splitPoint);
            boolean b = userActionCountQueryStateService.queryActionCounts(logBean.getDeviceId(), eventState, crossRangeParam);
            if (b) {
                /* **
                 * TODO 将查询结果插入缓存  [param.realCnt,条件start,条件end]
                 *******/
                continue;
            }

            // 如果上面不满足，则将参数时段更新[原start,分界点]，去clickhouse service查询(注意：realCnt会累加)
            crossRangeParam.setRangeStart(originRangeStart);
            crossRangeParam.setRangeEnd(splitPoint);
            boolean b1 = userActionCountQueryClickhouseService.queryActionCounts(logBean.getDeviceId(), eventState, crossRangeParam);
            /* **
             * TODO 将查询结果插入缓存  [param.realCnt,原start,原end]
             *******/
            if (!b1) return false;
        }
        return true;
    }


    /**
     * 控制sequence类条件查询路由
     */
    public boolean sequenceConditionQuery(LogBean logBean, RuleParam ruleParam) throws Exception {

        // 取出规则中的序列条件
        List<RuleAtomicParam> userActionSequenceParams = ruleParam.getUserActionSequenceParams();
        // 如果序列条件为空,则直接返回true
        if (userActionSequenceParams == null || userActionSequenceParams.size() < 1) return true;
        // 取出序列条件相关参数
        long originStart = userActionSequenceParams.get(0).getRangeStart();
        long originEnd = userActionSequenceParams.get(0).getRangeEnd();
        int totalSteps = userActionSequenceParams.size();

        /* 缓存查询处理：
         *   依据查询后的结果，如果已经完全匹配的条件，从规则中直接剔除
         *   如果是部分有效的，则将条件的时间窗口起始点更新为缓存有效窗口的结束点
         *****/
        String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), userActionSequenceParams); // 拼接bufferKey
        BufferResult bufferResult = bufferManager.getBufferData(bufferKey, originStart, originEnd, totalSteps); // 从redis中取数据
        switch (bufferResult.getBufferAvailableLevel()) {
            case PARTIAL_AVL: // 缓存部分有效，则设置maxStep到参数中，并截短条件序列及条件时间窗口
                // 截短条件序列，如：[A B C D] -> [B C D]
                List<RuleAtomicParam> newSequenceList = userActionSequenceParams.subList(bufferResult.getBufferValue(), totalSteps);
                // 截短条件时间窗口：[缓存end,原end]
                newSequenceList.get(0).setRangeStart(bufferResult.getBufferRangeEnd());
                // 将缓存value值，放入参数对象的maxStep中
                ruleParam.setUserActionSequenceQueriedMaxStep(bufferResult.getBufferValue());
                // 更新规则参数对象
                ruleParam.setUserActionSequenceParams(newSequenceList);
                break;
            case UN_AVL:  // 缓存无效，不作任何处理
                break;
            case WHOLE_AVL:  // 缓存完全匹配，则直接返回
                return true;
        }


        // 如果序列条件有内容，才开始计算
        userActionSequenceParams = ruleParam.getUserActionSequenceParams();
        if (userActionSequenceParams != null && userActionSequenceParams.size() > 0) {
            // 计算查询分界点timestamp ：当前时间对小时向上取整-2
            long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(), Calendar.HOUR), -2).getTime();

            /*  取出规则中序列条件的时间窗口起止点
             *  注意，此处取到的条件窗口，已经是经过缓存截短后的窗口
             *****/
            long rangeStart = userActionSequenceParams.get(0).getRangeStart();
            long rangeEnd = userActionSequenceParams.get(0).getRangeEnd();

            /* 开始分路控制，有如下3中可能性：
             *    a. 只查近期
             *    b. 只查远期
             *    c. 跨界查询
             ******/
            // a. 只查近期  如果 条件start>分界点，则仅在state中查询
            if (rangeStart >= splitPoint) {
                // 注意！这里查询到maxStep结果，会将缓存结果进行累加
                boolean b = userActionSequenceQueryStateService.queryActionSequence("", eventState, ruleParam);
                /*
                 * TODO 将查询结果插入缓存 [param.maxStep,原start,原end]
                 ******/
                return b;
            }

            //  b.只查远期:如果 条件end<分界点，则在clickhouse中查询
            else if (rangeEnd < splitPoint) {
                // 注意！这里查询到maxStep结果，会将缓存结果进行累加
                boolean b = userActionSequenceQueryClickhouseService.queryActionSequence(logBean.getDeviceId(), null, ruleParam);
                /*
                 * TODO 将查询结果插入缓存 [param.maxStep,原start,原end]
                 ******/
                return b;
            }

            // c.跨界查询:如果 条件start跨越分界点，则进行双路查询
            else {
                /**
                 * 1. 先查state碰运气
                 */
                // 修改时间窗口 [分界点,截短后条件的end]
                modifyTimeRange(userActionSequenceParams, splitPoint, rangeEnd);
                // 执行查询，maxStep会在缓存value基础上累加
                boolean b = userActionSequenceQueryStateService.queryActionSequence(logBean.getDeviceId(), eventState, ruleParam);
                if (b) {
                    /*
                     * TODO 将查询结果插入缓存 [ param.maxStep, 分界点, rangeEnd(截短后条件的end) ]
                     ******/
                    return true;
                }

                /**
                 * 2. 如果运气没碰上，则按常规流程查（先clickhouse，再state，再整合结果）
                 * 条件如：[A    B     C]
                 */
                // 修改时间窗口 [缓存截短后的rangeStart,分界点]
                modifyTimeRange(userActionSequenceParams, rangeStart, splitPoint);
                // 执行clickhouse查询，maxStep会在缓存value基础上累加
                boolean b1 = userActionSequenceQueryClickhouseService.queryActionSequence(logBean.getDeviceId(), eventState, ruleParam);
                int farMaxStep = ruleParam.getUserActionSequenceQueriedMaxStep();
                if (b1) {
                    /*
                     * TODO 将查询结果插入缓存 [ farMaxStep, 缓存截短后的rangeStart, 分界点 ]
                     ******/
                    return true;
                }


                /**
                 * 3. 如果远期部分不足以满足整个条件，则将条件再次截短（相对于缓存截短后的条件进行再次截短）
                 */
                // 修改时间窗口 [分界点,截短后的rangeEnd]
                modifyTimeRange(userActionSequenceParams, splitPoint, rangeEnd);
                // 在ck查询结果上再次截短条件序列
                ruleParam.setUserActionSequenceParams(userActionSequenceParams.subList(farMaxStep, userActionSequenceParams.size()));
                // 执行state查询，maxStep会在ck查询结果上（包含缓存value）累加
                boolean b2 = userActionSequenceQueryStateService.queryActionSequence(logBean.getDeviceId(), eventState, ruleParam);

                /*
                 * TODO 将查询结果插入缓存 [rule.maxStep, 原originStart, 原originEnd ]
                 ******/
                return ruleParam.getUserActionSequenceQueriedMaxStep() >= totalSteps;
            }

        }

        return true;
    }


    /**
     * 更新条件时间窗口起始点的工具方法
     *
     * @param userActionSequenceParams
     * @param newStart
     * @param newEnd
     */
    private void modifyTimeRange(List<RuleAtomicParam> userActionSequenceParams, long newStart, long newEnd) {
        for (RuleAtomicParam userActionSequenceParam : userActionSequenceParams) {
            userActionSequenceParam.setRangeStart(newStart);
            userActionSequenceParam.setRangeEnd(newEnd);
        }
    }
}
