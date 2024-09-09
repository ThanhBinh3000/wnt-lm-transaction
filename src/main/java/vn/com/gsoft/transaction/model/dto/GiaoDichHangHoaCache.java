package vn.com.gsoft.transaction.model.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class GiaoDichHangHoaCache {
    private String tenNhomNganhHang;
    private Long id ;
    private Long thuocId;
    private String tenThuoc;
    private Integer nhomDuocLyId;
    private Integer nhomHoatChatId;
    private BigDecimal soLuong;
    private BigDecimal giaNhap;
    private BigDecimal giaBan;
    private String tenDonVi;
    private Date ngayGiaoDich;
    private Integer LoaiGiaoDich;
    private String maCoSo;
    private Integer maPhieuChiTiet;
    private Integer nhomNganhHangId;
}
