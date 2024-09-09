package vn.com.gsoft.transaction.repository;

import jakarta.persistence.Tuple;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;

import java.util.Arrays;
import java.util.List;

@Repository
public interface GiaoDichHangHoaRepository extends BaseRepository<GiaoDichHangHoa, GiaoDichHangHoaReq, Long> {
    @Query(value = "SELECT * FROM GiaoDichHangHoa c "
            + "WHERE 1=1 "
            + " AND ((:#{#param.loaiGiaoDich} IS NULL) OR (c.LoaiGiaoDich = :#{#param.loaiGiaoDich})) "
            //+ " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich <= :#{#param.fromDate})"
            //+ " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})"
            //+ " AND ((:#{#param.dongBang} IS NULL) OR (c.DongBang = :#{#param.dongBang})) "
            + " ORDER BY c.ThuocId", nativeQuery = true
    )
    Page<GiaoDichHangHoa> searchPage(@Param("param") GiaoDichHangHoaReq param, Pageable pageable);

    @Query(value = "SELECT * FROM GiaoDichHangHoa c " +
            "WHERE 1=1 "
            + " AND ((:#{#param.loaiGiaoDich} IS NULL) OR (c.LoaiGiaoDich = :#{#param.loaiGiaoDich})) "
            + " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})"
            + " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})"
            + " AND ((:#{#param.dongBang} IS NULL) OR (c.DongBang = :#{#param.dongBang})) "
            + " AND ((:#{#param.nhomDuocLyId} IS NULL) OR (c.nhomDuocLyId = :#{#param.nhomDuocLyId})) "
            + " ORDER BY c.NgayGiaoDich desc", nativeQuery = true
    )
    List<GiaoDichHangHoa> searchList(@Param("param") GiaoDichHangHoaReq param);

    @Query(value = "SELECT TOP(:top) " +
            "'TenNhomNganhHang' AS tenNhomNganhHang, s.ThuocId, " +
            "'Tenhang' as 'tenThuoc', 'TenDonVi' as 'tenDonVi', 0.0 as 'GB', 0.0 as 'GN', 0.0 as 'GBCS', 0.0 as 'GNCS'" +
            ", s.Tong as 'soLieuThiTruong'" +
            ", (SELECT SUM(c1.GiaBan * c1.SoLuong) FROM GiaoDichHangHoa c1" +
            " WHERE 1=1" +
            " AND c1.MaCoSo = :#{#param.maCoSo}" +
            " AND c1.ThuocId = s.ThuocId" +
            " AND (:#{#param.fromDate} IS NULL OR c1.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c1.NgayGiaoDich <= :#{#param.toDate})) as 'soLieuCoSo' FROM " +
            "(SELECT c.ThuocId, SUM(c.GiaBan * c.SoLuong) as Tong FROM GiaoDichHangHoa c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDoanhThu(@Param("param") GiaoDichHangHoaReq param ,
                                            @Param("top") Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "'TenNhomNganhHang' AS tenNhomNganhHang, s.ThuocId, " +
            "'Tenhang' as 'tenThuoc', 'TenDonVi' as 'tenDonVi', 0.0 as 'GB', 0.0 as 'GN', 0.0 as 'GBCS', 0.0 as 'GNCS'" +
            ", s.Tong as 'soLieuThiTruong'" +
            ", (SELECT SUM(c1.SoLuong) FROM GiaoDichHangHoa c1" +
            " WHERE 1=1" +
            " AND c1.MaCoSo = :#{#param.maCoSo}" +
            " AND c1.ThuocId = s.ThuocId" +
            " AND (:#{#param.fromDate} IS NULL OR c1.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND ((:#{#param.loaiGiaoDich} IS NULL) OR (c1.LoaiGiaoDich = :#{#param.loaiGiaoDich})) "+
            " AND (:#{#param.toDate} IS NULL OR c1.NgayGiaoDich <= :#{#param.toDate})) as 'soLieuCoSo' FROM " +
            "(SELECT c.ThuocId, SUM(c.SoLuong) as Tong FROM GiaoDichHangHoa c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND (:#{#param.loaiGiaoDich} IS NULL OR c.LoaiGiaoDich = :#{#param.loaiGiaoDich}) "+
            " GROUP BY c.thuocId) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDoanhSoLuong(@Param("param") GiaoDichHangHoaReq param ,
                                   @Param("top") Integer top);
    @Query(value = "SELECT TOP(:top) " +
            "'TenNhomNganhHang' AS tenNhomNganhHang, s.ThuocId, " +
            "'Tenhang' as 'tenThuoc', 'TenDonVi' as 'tenDonVi', (s.GB - s.GN)/s.GN as 'soLieuThiTruong', 0.0 as 'soLieuCoSo'" +
            ", s.GN, s.GB" +
            ", (SELECT Max(c1.GiaBan) FROM GiaoDichHangHoa c1" +
            " WHERE 1=1" +
            " AND c1.MaCoSo = :#{#param.maCoSo}" +
            " AND c1.ThuocId = s.ThuocId" +
            " AND c1.LoaiGiaoDich = 2" +
            " AND (:#{#param.fromDate} IS NULL OR c1.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c1.NgayGiaoDich <= :#{#param.toDate})) as 'GBCS'" +
            ", (SELECT Min(c1.GiaNhap) FROM GiaoDichHangHoa c1" +
            " WHERE 1=1" +
            " AND c1.MaCoSo = :#{#param.maCoSo}" +
            " AND c1.ThuocId = s.ThuocId" +
            " AND c1.LoaiGiaoDich = 1" +
            " AND c1.GiaBan > 0 AND c1.GiaNhap > 0" +
            " AND (:#{#param.fromDate} IS NULL OR c1.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c1.NgayGiaoDich <= :#{#param.toDate})) as 'GNCS'" +
            " FROM " +
            "(SELECT c.ThuocId, " +
            " MIN(CASE WHEN c.LoaiGiaoDich = 1 THEN GiaNhap ELSE NULL END) as GN, " +
            " MAX(CASE WHEN c.LoaiGiaoDich = 2 THEN GiaBan ELSE NULL END) as GB" +
            " FROM GiaoDichHangHoa c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId) s" +
            " WHERE s.GN > 0 AND s.GN >= 1000" +
            " ORDER BY soLieuThiTruong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDoanhTSLN(@Param("param") GiaoDichHangHoaReq param,
                                    @Param("top") Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi, 0.0 as 'GB', 0.0 as 'GN', 0.0 as 'GBCS', 0.0 as 'GNCS'" +
            ", s.Tong as 'soLieuThiTruong'" +
            ", (SELECT SUM(c1.SoLuong) FROM GiaoDichHangHoa c1" +
            " WHERE 1=1" +
            " AND c1.MaCoSo = :#{#param.maCoSo}" +
            " AND c1.ThuocId = s.ThuocId" +
            " AND (:#{#param.fromDate} IS NULL OR c1.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c1.NgayGiaoDich <= :#{#param.toDate})) as 'soLieuCoSo' FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi, SUM(c.SoLuong) as Tong FROM GiaoDichHangHoa c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND (:#{#param.loaiGiaoDich} IS NULL OR c.LoaiGiaoDich = :#{#param.loaiGiaoDich}) "+
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDoanhSoLuongPushRedis(@Param("param") GiaoDichHangHoaReq param ,
                                       @Param("top") Integer top);
    @Query(value =
            "(SELECT c.ThuocId, SUM(c.SoLuong) as Tong FROM GiaoDichHangHoa c" +
            " WHERE 1=1 " +
            " AND c.MaCoSo = :#{#param.maCoSo}" +
            " AND c.thuocId in (:ids)" +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND (:#{#param.loaiGiaoDich} IS NULL OR c.LoaiGiaoDich = :#{#param.loaiGiaoDich}) "+
            " GROUP BY c.thuocId) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDoanhSoLuongByMaCoSo(@Param("param") GiaoDichHangHoaReq param ,
                                               @Param("ids") Long[] ids);

    @Query(value = "SELECT c.id, c.thuocid,c.tenThuoc," +
            "c.ngayGiaoDich, c.nhomDuocLyId, c.nhomNganhHangId," +
            "c.nhomHoatChatId, c.maCoso, c.loaiGiaoDich," +
            "c.soLuong, c.giaNhap, c.giaBan, c.tenDonVi," +
            "c.tenNhomNganhHang, c.maPhieuChiTiet, c.nhomHoatChatId FROM GiaoDichHangHoa c " +
            "WHERE 1=1 "
            + " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})"
            + " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})"
            + " ORDER BY c.NgayGiaoDich desc", nativeQuery = true
    )
    List<Tuple> searchListCache(@Param("param") GiaoDichHangHoaReq param);
}
