import cn.doitedu.dynamic_rule.pojo.DroolFact;
import cn.doitedu.dynamic_rule.pojo.LogBean;
import org.apache.commons.io.FileUtils;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.io.File;
import java.io.IOException;

public class RuleTest {

    public static void main(String[] args) throws IOException {


        String s = FileUtils.readFileToString(new File("dynamic_rule_engine/rules_drl/rule1.drl"), "utf-8");

        KieHelper kieHelper = new KieHelper();
        KieSession kieSession = kieHelper.addContent(s, ResourceType.DRL).build().newKieSession();


        DroolFact droolFact = new DroolFact();

        LogBean logBean = new LogBean();
        logBean.setEventId("D");

        droolFact.setLogBean(logBean);


        kieSession.insert(droolFact);
        kieSession.fireAllRules();


        kieSession.dispose();

    }
}
