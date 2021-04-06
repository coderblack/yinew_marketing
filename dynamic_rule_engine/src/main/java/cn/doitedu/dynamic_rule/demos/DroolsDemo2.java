package cn.doitedu.dynamic_rule.demos;

import org.apache.commons.io.FileUtils;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.io.File;
import java.io.IOException;

public class DroolsDemo2 {

    public static void main(String[] args) throws IOException {

        KieHelper kieHelper = new KieHelper();


        String drlFilePath = "E:\\yinew_marketing\\dynamic_rule_engine\\src\\main\\resources\\rules\\test.drl";
        String s = FileUtils.readFileToString(new File(drlFilePath), "utf-8");

        kieHelper.addContent(s, ResourceType.DRL);
        KieSession kieSession = kieHelper.build().newKieSession();


        Applicant 康康 = new Applicant("康康", 16);
        kieSession.insert(康康);
        kieSession.fireAllRules();


    }
}
