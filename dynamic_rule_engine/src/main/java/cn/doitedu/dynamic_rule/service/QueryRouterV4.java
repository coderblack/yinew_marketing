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
 * 核心计算入口类
 *   1. 先用缓存管理器去查询缓存数据
 *   2. 然后再根据情况调用各类service去计算
 *   3. 还要将计算结果更新到缓存中
 *
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

    /**
     * 构造方法
     * 创建buffermanager实例，及各类其他service实例
     * @param eventState
     * @throws Exception
     */
    public QueryRouterV4(ListState<LogBean> eventState) throws Exception {

        userProfileQueryService = new UserProfileQueryServiceHbaseImpl();

        /**
         * 构造STATE查询服务
         */
        userActionCountQueryStateService = new UserActionCountQueryServiceStateImpl(eventState);
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

    /**
     * 控制画像条件查询路由
     * @param logBean
     * @param ruleParam
     * @return
     */
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
        updateRuleParamByBufferResult(logBean, userActionCountParams);


        // 计算查询分界点timestamp ：当前时间对小时取整，-1
        long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(), Calendar.HOUR), -2).getTime();

        // 将规则count条件list，按照时间窗口范围，划分成3个子list
        ArrayList<RuleAtomicParam> farRangeParamList = new ArrayList<>();  // 远期条件组
        ArrayList<RuleAtomicParam> nearRangeParamList = new ArrayList<>();  // 近期条件组
        ArrayList<RuleAtomicParam> crossRangeParamList = new ArrayList<>();  // 跨界条件组
        // 调用方法划分
        splitParamList(userActionCountParams, splitPoint, farRangeParamList, nearRangeParamList, crossRangeParamList);

        /*
         * 近期条件组，批量查询
         *****/
        if (nearRangeParamList.size() > 0) {
            // 将规则总参数对象中的“次数类条件”覆盖成： 近期条件组
            ruleParam.setUserActionCountParams(nearRangeParamList);
            // 交给stateService对这一组条件进行计算
            boolean countMatch = userActionCountQueryStateService.queryActionCounts("",  ruleParam);

            /* **
             *  将查询结果插入缓存
             *******/
            for (RuleAtomicParam ruleAtomicParam : nearRangeParamList) {
                String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), ruleAtomicParam);
                bufferManager.putBufferData(bufferKey,ruleAtomicParam.getRealCnts(),ruleAtomicParam.getRangeStart(),ruleAtomicParam.getRangeEnd());
            }

            if (!countMatch) return false;
        }


        /**
         * 跨界count条件组查询
         * 叶子条件逐一控制查询
         */
        for (RuleAtomicParam crossRangeParam : crossRangeParamList) {
            long originRangeStart = crossRangeParam.getRangeStart();
            long originRangeEnd = crossRangeParam.getRangeEnd();
            String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), crossRangeParam);

            // 将参数时段替换[分界点,原end]，去state service中查询
            crossRangeParam.setRangeStart(splitPoint);
            boolean b = userActionCountQueryStateService.queryActionCounts(logBean.getDeviceId(), crossRangeParam);
            if (b) {
                /* **
                 *  将查询结果插入缓存  [param.realCnt,条件start,条件end]
                 *******/

                bufferManager.putBufferData(bufferKey,crossRangeParam.getRealCnts(),crossRangeParam.getRangeStart(),crossRangeParam.getRangeEnd());
                continue;
            }

            // 如果上面不满足，则将参数时段更新[原start,分界点]，去clickhouse service查询(注意：realCnt会累加)
            crossRangeParam.setRangeStart(originRangeStart);
            crossRangeParam.setRangeEnd(splitPoint);
            boolean b1 = userActionCountQueryClickhouseService.queryActionCounts(logBean.getDeviceId(), crossRangeParam);
            /* **
             *  将查询结果插入缓存  [param.realCnt,原start,原end]
             *******/
            bufferManager.putBufferData(bufferKey,crossRangeParam.getRealCnts(),originRangeStart,originRangeEnd);

            if (!b1) return false;
        }


        /*
         * 远期条件组，批量打包查询
         ****/
        if (farRangeParamList.size() > 0) {
            // 将规则总参数对象中的“次数类条件”覆盖成： 远期条件组
            ruleParam.setUserActionCountParams(farRangeParamList);
            boolean b = userActionCountQueryClickhouseService.queryActionCounts(logBean.getDeviceId(),  ruleParam);

            /* **
             *  将查询结果插入缓存
             *******/
            for (RuleAtomicParam ruleAtomicParam : farRangeParamList) {
                String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), ruleAtomicParam);
                bufferManager.putBufferData(bufferKey,ruleAtomicParam.getRealCnts(),ruleAtomicParam.getRangeStart(),ruleAtomicParam.getRangeEnd());
            }

            if (!b) return false;
        }


        return true;
    }



    /**
     * 控制sequence类条件查询路由
     * @param logBean 事件bean
     * @param ruleParam 规则参数
     * @return 是否匹配
     * @throws Exception 异常
     */
    public boolean sequenceConditionQuery(LogBean logBean, RuleParam ruleParam) throws Exception {

        // 取出规则中的序列条件
        List<RuleAtomicParam> originSequenceParamList = ruleParam.getUserActionSequenceParams();
        // 如果序列条件为空,则直接返回true
        if (originSequenceParamList == null || originSequenceParamList.size() < 1) return true;
        // 取出序列条件相关参数
        long originStart = originSequenceParamList.get(0).getRangeStart();
        long originEnd = originSequenceParamList.get(0).getRangeEnd();
        int totalSteps = originSequenceParamList.size();

        /* 缓存查询处理：
         *   依据查询后的结果，如果已经完全匹配的条件，则直接返回最终结果true
         *   如果是部分有效的，则将条件的时间窗口起始点更新为缓存有效窗口的结束点，并截断条件序列
         *   比如，原始条件是  [A  B   C   D](t1,t10)，
         *        在缓存中查到  [A   B](t1,t5)
         *        那么，我们就要把后续的处理条件修改：  [C   D](t6,t10)
         *****/
        String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), originSequenceParamList); // 拼接bufferKey
        BufferResult bufferResult = bufferManager.getBufferData(bufferKey, originStart, originEnd, totalSteps); // 从redis中取数据
        switch (bufferResult.getBufferAvailableLevel()) {
            case PARTIAL_AVL: // 缓存部分有效，则设置maxStep到参数中，并截短条件序列及条件时间窗口
                // 截短条件序列，如：[A B C D] -> [C D]
                List<RuleAtomicParam> newSequenceList = originSequenceParamList.subList(bufferResult.getBufferValue(), totalSteps);
                // 截短条件时间窗口：[缓存end,原end]
                modifyTimeRange(newSequenceList,bufferResult.getBufferRangeEnd(),originEnd);
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
        List<RuleAtomicParam> newParamList = ruleParam.getUserActionSequenceParams();
        if (newParamList != null && newParamList.size() > 0) {
            // 计算查询分界点timestamp ：当前时间对小时向上取整-2
            long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(), Calendar.HOUR), -2).getTime();

            /*  取出规则中序列条件的时间窗口起止点
             *  注意，此处取到的条件窗口，已经是经过缓存截短后的窗口
             *****/
            long rangeStart = newParamList.get(0).getRangeStart();
            long rangeEnd = newParamList.get(0).getRangeEnd();

            /* 开始分路控制，有如下3中可能性：
             *  a. 只查近期
             *  b. 跨界查询
             *  c. 只查远期
             ******/
            // a. 只查近期 ：条件start>分界点，则仅在state中查询
            if (rangeStart >= splitPoint) {
                // 注意！这里查询到maxStep结果，会将缓存结果进行累加
                boolean b = userActionSequenceQueryStateService.queryActionSequence("", eventState, ruleParam);

                // 将查询结果插入缓存 ==>   key:初始条件，value： [param.maxStep|原始start,原始end]
                String bufferKey1 = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), originSequenceParamList);
                bufferManager.putBufferData(bufferKey1,ruleParam.getUserActionSequenceQueriedMaxStep(),originStart,originEnd);

                return b;
            }

            // b.跨界查询 ： 条件start跨越分界点，则进行双路查询
            else if(rangeStart<splitPoint && rangeEnd> splitPoint){
                /**
                 * 1. 先查state碰运气
                 */
                // 修改时间窗口 [分界点,截短后条件的end]
                modifyTimeRange(newParamList, splitPoint, rangeEnd);

                // 记录下buffer查询后的值，以便后面恢复现场
                int bufferValue = ruleParam.getUserActionSequenceQueriedMaxStep();

                // 执行查询，maxStep会在缓存value基础上累加
                userActionSequenceQueryStateService.queryActionSequence(logBean.getDeviceId(), eventState, ruleParam);
                if (ruleParam.getUserActionSequenceQueriedMaxStep()>=totalSteps) {
                    /*
                     * 将查询结果插入缓存 [ param.maxStep, 初始start, 初始end ]
                     ******/
                    String bufferKey1 = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), originSequenceParamList);
                    bufferManager.putBufferData(bufferKey1,ruleParam.getUserActionSequenceQueriedMaxStep(),originStart,originEnd);

                    return true;
                }else{
                    // 将参数中的maxStep恢复到缓存查完后的值
                    ruleParam.setUserActionSequenceQueriedMaxStep(bufferValue);
                }

                /**
                 * 2. 如果运气没碰上，则按常规流程查（先clickhouse，再state，再整合结果）
                 */
                // 修改条件时间窗口 [缓存截短后的rangeStart,分界点] 去clickhouse中查
                modifyTimeRange(newParamList, rangeStart, splitPoint);

                // 执行clickhouse查询，maxStep会在缓存value基础上累加
                boolean b1 = userActionSequenceQueryClickhouseService.queryActionSequence(logBean.getDeviceId(), eventState, ruleParam);
                int farMaxStep = ruleParam.getUserActionSequenceQueriedMaxStep();
                if (ruleParam.getUserActionSequenceQueriedMaxStep() >= totalSteps) {
                    /*
                     *  将查询结果插入缓存 [ farMaxStep, 缓存截短后的rangeStart, 分界点 ]
                     ******/
                    String bufferKey1 = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), originSequenceParamList);
                    bufferManager.putBufferData(bufferKey1,ruleParam.getUserActionSequenceQueriedMaxStep(),originStart,splitPoint);

                    return true;
                }


                /**
                 * 3. 如果远期部分不足以满足整个条件，则将条件再次截短（相对于缓存截短后的条件进行再次截短）
                 */
                // 修改时间窗口 [分界点,截短后的rangeEnd]
                modifyTimeRange(newParamList, splitPoint, rangeEnd);
                // 在ck查询结果上再次截短条件序列
                ruleParam.setUserActionSequenceParams(originSequenceParamList.subList(farMaxStep, originSequenceParamList.size()));
                // 执行state查询，maxStep会在ck查询结果上（包含缓存value）累加
                userActionSequenceQueryStateService.queryActionSequence(logBean.getDeviceId(), eventState, ruleParam);

                // 将查询结果插入缓存 [rule.maxStep, 原originStart, 原originEnd ]
                String bufferKey1 = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), originSequenceParamList);
                bufferManager.putBufferData(bufferKey1,ruleParam.getUserActionSequenceQueriedMaxStep(),originStart,originEnd);
                return ruleParam.getUserActionSequenceQueriedMaxStep() >= totalSteps;
            }


            //  b.只查远期 ： 条件end<分界点，则在clickhouse中查询
            else {
                // 注意！这里查询到maxStep结果，会将缓存结果进行累加
                boolean b = userActionSequenceQueryClickhouseService.queryActionSequence(logBean.getDeviceId(), null, ruleParam);

                // 将查询结果插入缓存 [param.maxStep,原start,原end]
                String bufferKey1 = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), originSequenceParamList);
                List<RuleAtomicParam> lst = ruleParam.getUserActionSequenceParams();
                bufferManager.putBufferData(bufferKey1,ruleParam.getUserActionSequenceQueriedMaxStep(),lst.get(0).getRangeStart(),lst.get(0).getRangeEnd());
                return b;
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


    /**
     * 查询缓存，并根据缓存结果情况，更新原始规则条件：
     * 1. 缩短条件的窗口
     * 2. 剔除条件
     * 3. 啥也不做
     * @param logBean 待处理事件
     * @param userActionCountParams count类条件list
     */
    private void updateRuleParamByBufferResult(LogBean logBean, List<RuleAtomicParam> userActionCountParams) {
        for (int i = 0; i < userActionCountParams.size(); i++) {

            // 从条件list中取出条件i
            RuleAtomicParam countParam = userActionCountParams.get(i);
            // 拼接bufferKey
            String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), countParam);

            // 从redis中取数据
            BufferResult bufferResult = bufferManager.getBufferData(bufferKey, countParam);

            switch (bufferResult.getBufferAvailableLevel()) {
                // 如果是部分有效
                case PARTIAL_AVL:
                    // 则更新规则条件的窗口起始点
                    countParam.setRangeStart(bufferResult.getBufferRangeEnd());
                    // 将value值，放入参数对象的realCnt中
                    countParam.setRealCnts(bufferResult.getBufferValue());
                    break;
                // 如果是完全有效，则剔除该条件
                case WHOLE_AVL:
                    userActionCountParams.remove(i);
                    i--;
                    break;
                case UN_AVL:
            }

        }
    }


    /**
     *
     * @param userActionCountParams  初始条件List
     * @param splitPoint  时间分界点
     * @param farRangeParams  远期条件组
     * @param nearRangeParams 近期条件组
     * @param crossRangeParams 跨界条件组
     */
    private void splitParamList(List<RuleAtomicParam> userActionCountParams, long splitPoint, ArrayList<RuleAtomicParam> farRangeParams, ArrayList<RuleAtomicParam> nearRangeParams, ArrayList<RuleAtomicParam> crossRangeParams) {
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
    }

}
