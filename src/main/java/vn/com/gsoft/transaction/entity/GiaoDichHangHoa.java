package vn.com.gsoft.transaction.entity;

import jakarta.persistence.*;
import jakarta.persistence.Entity;
import lombok.*;

import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "GiaoDichHangHoa")

public class GiaoDichHangHoa extends BaseEntity {
    @Id
    @Column(name = "Id")
    private Long id ;
    @Column(name = "ThuocId")
    private Long thuocId;
    @Column(name = "TenThuoc")
    private String tenThuoc;
    @Column(name = "NhomThuocId")
    private Integer nhomThuocId;
    @Column(name = "TenNhomThuoc")
    private String tenNhomThuoc;
    @Column(name = "NhomDuocLyId")
    private Integer nhomDuocLyId;
    @Column(name = "TenNhomDuocLy")
    private String tenNhomDuocLy;
    @Column(name = "SoLuong")
    private BigDecimal soLuong;
    @Column(name = "GiaNhap")
    private BigDecimal giaNhap;
    @Column(name = "GiaBan")
    private BigDecimal giaBan;
    @Column(name = "TenDonVi")
    private String tenDonVi;
    @Column(name = "TenHoatChat")
    private String tenHoatChat;
    @Column(name = "NgayGiaoDich")
    private Date ngayGiaoDich;
    @Column(name = "DongBang")
    private Boolean dongBang;
    @Column(name = "LoaiGiaoDich")
    private Integer LoaiGiaoDich;
    @Column(name = "MaCoSo")
    private String maCoSo;
    @Column(name = "SoLuongQuyDoi")
    private BigDecimal soLuongQuyDoi;
    @Transient
    private BigDecimal doanhThu;

    public GiaoDichHangHoa(String tenThuoc, String tenNhomThuoc, String tenDonVi, BigDecimal doanhThu) {
        this.tenThuoc = tenThuoc;
        this.tenNhomThuoc = tenNhomThuoc;
        this.tenDonVi = tenDonVi;
        this.doanhThu = doanhThu;
    }
}
