package vn.com.gsoft.transaction.repository;

import jakarta.persistence.Tuple;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;

import java.util.List;
import java.util.Optional;

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

    //region So Lieu Co So
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
    @Query(value = "SELECT c.ThuocId as 'thuocId'" +
            ", (CASE WHEN SUM(c.tongBanVoiGiaNhap) > 0 THEN ((SUM(c.tongBan) - SUM(c.tongBanVoiGiaNhap)) / SUM(c.tongBanVoiGiaNhap)) * 100 ELSE 0.0 END) as 'soLuong' " +
            ", 0.0 as 'nhap', 0.0 as 'ban'" +
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
    //endregion

    //region NGAY_THANG
    @Query(value = "DECLARE @query nvarchar(1024) =:query" +
            " exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> groupByTopSL_T_ANY(@Param("query") String query);

    @Query(value = "DECLARE @query nvarchar(1024) =:query" +
            " exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDT_T_ANY(@Param("query") String query);

    @Query(value = "DECLARE @query nvarchar(1024) =:query" +
            " exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> groupByTopTSLN_T_ANY(@Param("query") String query);
    //endregion

    //region THÁNG NĂM
    @Query(value = "DECLARE @query nvarchar(1024) =:query" +
            " exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> groupByTopSL_T0(@Param("query") String query);

    @Query(value = "DECLARE @query nvarchar(1024) =:query" +
            " exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> groupByTopDT_T0(@Param("query") String query);

    @Query(value = "DECLARE @query nvarchar(1024) =:query" +
            " exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> groupByTopTSLN_T0(@Param("query") String query);

    @Query(value = "DECLARE @query nvarchar(1024) =:query " +
            "exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> searchListTop_DT_T0(@Param("query") String query);

    @Query(value ="DECLARE @query nvarchar(1024) =:query " +
            "exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> searchListTop_SL_T0(@Param("query") String query);

    @Query(value = "DECLARE @query nvarchar(1024) =:query " +
                    "exec sp_executesql @query"
            , nativeQuery = true
    )
    List<Tuple> searchListTop_TSLN_T0(@Param("query") String query);
    //endregion

    //check table có tồn tại không
    @Query(value = "select c.name from LMNTDB.sys.tables c where" +
            " 1=1 AND" +
            " c.name = :tableName", nativeQuery = true)
    Optional<Tuple> checkTableExit(@Param("tableName") String tableName);
}
