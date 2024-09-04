package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class TopMatHangRes {
    private String tenThuoc;
    private String tenNhomNganhHang;
    private String tenDonVi;
    private BigDecimal soLieuThiTruong;
    private BigDecimal soLieuCoSo;
    private Long ThuocId;
    private BigDecimal GB;
    private BigDecimal GN;
    private BigDecimal GBCS;
    private BigDecimal GNCS;
}
