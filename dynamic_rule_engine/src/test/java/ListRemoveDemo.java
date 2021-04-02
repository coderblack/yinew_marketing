import java.util.ArrayList;

public class ListRemoveDemo {
    public static void main(String[] args) {

        ArrayList<String> lst = new ArrayList<>();
        lst.add("a");
 /*       lst.add("b");
        lst.add("c");
        lst.add("d");
        lst.add("e");*/

        /*for (String s : lst) {
            if(s.equals("c")) lst.remove("c");
        }*/


        for (int i = 0; i < lst.size(); i++) {

            String s = lst.get(i);
            if (s.equals("a") || s.equals("d")) {
                lst.remove(i);
                i--;
            }

        }

        System.out.println(lst);
    }
}
