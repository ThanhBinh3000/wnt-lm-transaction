package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;

import java.math.BigDecimal;
import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
public class TopMatHangRes extends BaseRequest {
    private String tenThuoc;
    private String tenNhomThuoc;
    private String tenDonVi;
    private BigDecimal soLieuThiTruong;
    private BigDecimal soLieuCoSo;
    public TopMatHangRes(String tenThuoc, String tenNhomThuoc, String tenDonVi,
                         BigDecimal soLieuThiTruong, BigDecimal soLieuCoSo ) {
        this.tenThuoc = tenThuoc;
        this.tenNhomThuoc = tenNhomThuoc;
        this.tenDonVi = tenDonVi;
        this.soLieuThiTruong = soLieuThiTruong;
        this.soLieuCoSo = soLieuCoSo;
    }
}
