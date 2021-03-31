import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

public class Test {
    public static void main(String[] args) {

        /*for(int i=0;i<100;i++){
            System.out.println(RandomStringUtils.randomAlphabetic(1));
        }*/

        long x = System.currentTimeMillis() - 2*60*60*1000;
        System.out.println(x);


        Date date = DateUtils.addHours(DateUtils.ceiling(new Date(), Calendar.HOUR), -2);
        System.out.println(date);






    }
}
