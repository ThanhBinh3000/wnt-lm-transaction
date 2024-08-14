package vn.com.gsoft.transaction.util.system;

import java.util.HashMap;
import java.util.Map;

public class ZNSUtils {
   public static String getResultByZNSCode(int code)
{
    Map<Integer, String> messages = new HashMap<>();
    messages.put(-106, "-106: Phương thức không được hỗ trợ");
    messages.put(-108, "-108: Số điện thoại không hợp lệ");
    messages.put(-109, "-109: ID mẫu ZNS không hợp lệ");
    messages.put(-112, "-112: Nội dung mẫu ZNS không hợp lệ");
    messages.put(-114, "-114: Người dùng không nhận được ZNS vì các lý do: Trạng thái tài khoản, Tùy chọn nhận ZNS, Sử dụng Zalo phiên bản cũ, hoặc các lỗi nội bộ khác");
    messages.put(-115, "-115: Tài khoản ZNS không đủ số dư");
    messages.put(-117, "-117: OA hoặc ứng dụng gửi ZNS chưa được cấp quyền sử dụng mẫu ZNS này");
    messages.put(-118, "-118: Tài khoản Zalo không tồn tại hoặc đã bị vô hiệu hoá");
    messages.put(-119, "-119: Tài khoản không thể nhận ZNS");

    return messages.getOrDefault(code, "Gửi thành công");
}
}
