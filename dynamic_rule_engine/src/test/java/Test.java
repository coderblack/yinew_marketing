import org.apache.commons.lang3.RandomStringUtils;

public class Test {
    public static void main(String[] args) {

        for(int i=0;i<100;i++){
            System.out.println(RandomStringUtils.randomAlphabetic(1));
        }
    }
}
