package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;

import java.math.BigDecimal;
import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
public class GiaoDichHangHoaRes extends BaseRequest {
    private Long id ;
    private Long thuocId;
    private String tenThuoc;
    private Integer nhomThuocId;
    private String tenNhomThuoc;
    private Integer nhomDuocLyId;
    private String tenNhomDuocLy;
    private BigDecimal soLuong;
    private BigDecimal giaNhap;
    private BigDecimal giaBan;
    private String tenDonVi;
    private String tenHoatChat;
    private Date ngayGiaoDich;
    private Boolean dongBang;
    private Integer LoaiGiaoDich;
    private String maCoSo;
    private BigDecimal soLuongQuyDoi;
    private BigDecimal doanhThuThiTruong;
    private BigDecimal tslnThiTruong;
    private BigDecimal soLuongThiTruong;
    private BigDecimal doanhThuCoSo;
    private BigDecimal tslnCoSo;
    private BigDecimal soLuongCoSo;
}
