package vn.com.gsoft.transaction.util.system;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StoreHelper {
    public static String getCodeBasedOnNumber(Long lastIdNumber)
    {
        String code;
        if (lastIdNumber < 10)
            code = String.format("000%s", lastIdNumber);
        else if (lastIdNumber < 100)
            code = String.format("00%s", lastIdNumber);
        else if (lastIdNumber < 1000)
            code = String.format("0%s", lastIdNumber);
        else
            code = String.format("%s", lastIdNumber);
        return code;
    }
}
