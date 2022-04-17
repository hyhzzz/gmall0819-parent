import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author coderhyh
 * @create 2022-04-17 2:10
 */
public class Test {
    public static void main(String[] args) throws ParseException {

        //String str = "2019年06月06日 16时03分14秒 545毫秒  星期四 +0800";
        //SimpleDateFormat sf = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒 SSS毫秒  E Z");
        //Date d = sf.parse(str);
        //System.out.println(d);


        Date d = new Date();

        SimpleDateFormat sf = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒 SSS毫秒  E Z");
        //把Date日期转成字符串，按照指定的格式转
        String str = sf.format(d);
        System.out.println(str);

    }
}
