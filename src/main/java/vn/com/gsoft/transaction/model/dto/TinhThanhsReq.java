package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;

@EqualsAndHashCode(callSuper = true)
@Data
public class TinhThanhsReq extends BaseRequest {
    private String maTinhThanh;
    private String tenTinhThanh;
}
