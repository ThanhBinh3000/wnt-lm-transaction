package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;
@EqualsAndHashCode(callSuper = true)
@Data
public class RoleReq extends BaseRequest {
    private String roleName;
    private Boolean isDeleted;
    private String maNhaThuoc;
    private String description;
    private Integer type;
}
