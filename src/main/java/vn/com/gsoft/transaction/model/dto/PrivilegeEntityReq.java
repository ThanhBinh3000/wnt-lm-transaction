package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;

@EqualsAndHashCode(callSuper = true)
@Data
public class PrivilegeEntityReq extends BaseRequest {
    private Long privilegeId;
    private Long entityId;
}
