package vn.com.gsoft.transaction.repository;

import jakarta.persistence.Tuple;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa_T1_2024;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa_T2_2024;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa_T3_2024;
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
            + " AND ((:#{#param.nhomDuocLyId} IS NULL) OR (c.nhomDuocLyId = :#{#param.nhomDuocLyId})) " +
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) "
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
            + " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) "
            + " ORDER BY c.NgayGiaoDich desc", nativeQuery = true
    )
    List<Tuple> searchListCache(@Param("param") GiaoDichHangHoaReq param);

    @Query(value = "SELECT c.ThuocId as 'thuocId', SUM(c.GiaBan * c.SoLuong) as 'ban' , 0.0 as 'nhap', 0.0 as 'soLuong'" +
            " FROM GiaoDichHangHoa c" +
            " WHERE 1=1 " +
            " AND c.maCoSo = :#{#param.maCoSo}" +
            " AND c.ThuocId in (:#{#param.thuocIds})" +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDoanhThuCS(@Param("param") GiaoDichHangHoaReq param);
    @Query(value = "SELECT c.ThuocId as 'thuocId', SUM(CASE WHEN c.LoaiGiaoDich = 2 THEN SoLuong ELSE 0.0 END) as 'soLuong' , 0.0 as 'nhap', 0.0 as 'ban'" +
            " FROM GiaoDichHangHoa c" +
            " WHERE 1=1 " +
            " AND c.maCoSo = :#{#param.maCoSo}" +
            " AND c.ThuocId in (:#{#param.thuocIds})" +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId"
            , nativeQuery = true
    )
    List<Tuple> groupByTopSLCS(@Param("param") GiaoDichHangHoaReq param);
    @Query(value = "SELECT c.ThuocId as 'thuocId', MAX(c.tsln) as 'soLuong' , 0.0 as 'nhap', 0.0 as 'ban'" +
            " FROM GiaoDichHangHoa c" +
            " WHERE 1=1 " +
            " AND c.maCoSo = :#{#param.maCoSo}" +
            " AND c.ThuocId in (:#{#param.thuocIds})" +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId"
            , nativeQuery = true
    )
    List<Tuple> groupByTopTSLNCS(@Param("param") GiaoDichHangHoaReq param);

    //region THÁNG 1-2024
    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.TongSoLuong) as Tong FROM GiaoDichHangHoa_T1_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopSLT1_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.tongBan) as Tong FROM GiaoDichHangHoa_T1_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDTT1_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", MAX(c.TSLN) as Tong FROM GiaoDichHangHoa_T1_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND c.tsln < 500"+
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopTSLNT1_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);
    @Query(value =
            "(SELECT c FROM GiaoDichHangHoa_T1_2024 c" +
                    " WHERE 1=1 " +
                    " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
                    " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
                    " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
                    " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
                    " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
                    " AND (:#{#param.loaiGiaoDich} IS NULL OR c.LoaiGiaoDich = :#{#param.loaiGiaoDich}) "+
                    " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> searchListT1_2024(@Param("param") GiaoDichHangHoaReq param);
    //endregion
    //region THÁNG 2-2024
    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.TongSoLuong) as Tong FROM GiaoDichHangHoa_T2_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopSLT2_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.tongBan) as Tong FROM GiaoDichHangHoa_T2_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDTT2_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", MAX(c.TSLN) as Tong FROM GiaoDichHangHoa_T2_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND c.tsln < 500"+
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopTSLNT2_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);
    @Query(value =
            "(SELECT c FROM GiaoDichHangHoa_T2_2024 c" +
                    " WHERE 1=1 " +
                    " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
                    " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
                    " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
                    " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
                    " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
                    " AND (:#{#param.loaiGiaoDich} IS NULL OR c.LoaiGiaoDich = :#{#param.loaiGiaoDich}) "+
                    " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<GiaoDichHangHoa_T2_2024> searchListT2_2024(@Param("param") GiaoDichHangHoaReq param);
    //endregion
    //region THÁNG 3-2024
    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.TongSoLuong) as Tong FROM GiaoDichHangHoa_T3_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopSLT3_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.tongBan) as Tong FROM GiaoDichHangHoa_T3_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDT_T3_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", MAX(c.TSLN) as Tong FROM GiaoDichHangHoa_T3_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND c.tsln < 500"+
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopTSLN_T3_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);
    @Query(value =
            "(SELECT c FROM GiaoDichHangHoa_T3_2024 c" +
                    " WHERE 1=1 " +
                    " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
                    " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
                    " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
                    " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
                    " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
                    " AND (:#{#param.loaiGiaoDich} IS NULL OR c.LoaiGiaoDich = :#{#param.loaiGiaoDich}) "+
                    " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<GiaoDichHangHoa_T3_2024> searchListT3_2024(@Param("param") GiaoDichHangHoaReq param);
    //endregion
    //region THÁNG 0_2024
    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.TongSoLuong) as Tong FROM GiaoDichHangHoa_T0_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND (:#{#param.type} IS NULL OR c.type >= :#{#param.type}) " +
            " AND (:#{#param.type} IS NULL OR c.type <= :#{#param.type}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopSLT0_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.tongBan) as Tong FROM GiaoDichHangHoa_T0_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND (:#{#param.type} IS NULL OR c.type >= :#{#param.type}) " +
            " AND (:#{#param.type} IS NULL OR c.type <= :#{#param.type}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDTT0_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi" +
            ", s.Tong as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", MAX(c.TSLN) as Tong FROM GiaoDichHangHoa_T0_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " AND (:#{#param.type} IS NULL OR c.type >= :#{#param.type}) " +
            " AND (:#{#param.type} IS NULL OR c.type <= :#{#param.type}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY s.Tong desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopTSLNT0_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);
    @Query(value =
            "SELECT TOP(:top)" +
                    "c.tenNhomNganhHang, c.ThuocId, " +
                    "c.tenThuoc, c.tenDonVi" +
                    ", c.TongBan as 'soLieuThiTruong'" +
                    ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                    "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                    " FROM GiaoDichHangHoa_T0_2024 c" +
                    " WHERE 1=1 " +
                    " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
                    " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
                    " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
                    " AND (:#{#param.type} IS NULL OR c.type = :#{#param.type}) " +
                    " ORDER BY c.TongBan desc", nativeQuery = true
    )
    List<Tuple> searchListTop_DT_T0_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value =
            "SELECT TOP(:top) " +
                    "c.tenNhomNganhHang, c.ThuocId, " +
                    "c.tenThuoc, c.tenDonVi" +
                    ", c.TongSoLuong as 'soLieuThiTruong'" +
                    ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                    "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                    " FROM GiaoDichHangHoa_T0_2024 c" +
                    " WHERE 1=1 " +
                    " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
                    " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
                    " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
                    " AND (:#{#param.type} IS NULL OR c.type = :#{#param.type}) " +
                    " ORDER BY c.TongSoLuong desc"
            , nativeQuery = true
    )
    List<Tuple> searchListTop_SL_T0_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);
    @Query(value =
            "SELECT TOP(:top) " +
                    "c.tenNhomNganhHang, c.ThuocId, " +
                    "c.tenThuoc, c.tenDonVi ," +
                    "(CASE WHEN c.tongNhap > THEN ((tongBan - tongNhap) / tongNhap) * 100 ELSE NULL END)  as 'soLieuThiTruong'" +
                    ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                    "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                    " FROM GiaoDichHangHoa_T0_2024 c" +
                    " WHERE 1=1 " +
                    " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
                    " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
                    " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
                    " AND (:#{#param.type} IS NULL OR c.type = :#{#param.type}) " +
                    " ORDER BY soLieuThiTruong desc"
            , nativeQuery = true
    )
    List<Tuple> searchListTop_TSLN_T0_2024(@Param("param") GiaoDichHangHoaReq param, Integer top);
    //endregion
}
