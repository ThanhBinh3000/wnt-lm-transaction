package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;

@EqualsAndHashCode(callSuper = true)
@Data
public class UserProfileReq extends BaseRequest {
    private String userName;
    private String password;
    private String tenDayDu;
    private String email;
    private String soDienThoai;
    private String maNhaThuoc;
    private Boolean hoatDong;
    private String soCMT;
    private Boolean enableNT;
    private Long archivedId;
    private Long storeId;
    private Long regionId;
    private Long cityId;
    private Long wardId;
    private String addresses;
    private String tokenDevice;
    private String tokenBrowser;
    private Boolean isVerificationAccount;
    private String tokenDevice2;
    private String mobileDeviceId;
    private Long entityId;
    private String roleName;
}
