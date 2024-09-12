package vn.com.gsoft.transaction.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.Transient;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

@Data
public class HangHoaDaTinhToanCache implements Serializable {
    private String tenNhomNganhHang;
    private Long thuocId;
    private String tenThuoc;
    @Transient
    private Integer nhomDuocLyId;
    @Transient
    private Integer nhomHoatChatId;
    private BigDecimal soLuong;
    private BigDecimal tongNhap;
    private BigDecimal tongBan;
    private String tenDonVi;
    @Transient
    private Integer nhomNganhHangId;
    @Transient
    private BigDecimal soLieuThiTruong;
    @Transient
    private BigDecimal soLieuCoSo;
}
