package vn.com.gsoft.transaction.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.math.BigDecimal;
import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "GiaoDichHangHoa_T4_2024")

public class GiaoDichHangHoa_T4_2024 extends BaseEntity {
    @Id
    @Column(name = "Id")
    private Long id ;
    @Column(name = "ThuocId")
    private Long thuocId;
    @Column(name = "TenThuoc")
    private String tenThuoc;
    @Column(name = "NhomDuocLyId")
    private Integer nhomDuocLyId;
    @Column(name = "TongSoLuong")
    private BigDecimal tongSoLuong;
    @Column(name = "TongNhap")
    private BigDecimal tongNhap;
    @Column(name = "TongBan")
    private BigDecimal tongBan;
    @Column(name = "TenDonVi")
    private String tenDonVi;
    private Date ngayGiaoDich;
    @Column(name = "NhomHoatChatId")
    private Integer nhomHoatChatId;
    @Column(name = "NhomNganhHangId")
    private Integer nhomNganhHangId;
    @Column(name = "TenNhomNganhHang")
    private String tenNhomNganhHang;
    @Column(name = "TSLN")
    private BigDecimal tsln;
}
