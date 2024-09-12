package vn.com.gsoft.transaction.repository;

import jakarta.persistence.Tuple;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa_T1_2024;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa_T2_2024;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa_T8_2024;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;

import java.util.List;

@Repository
public interface GiaoDichHH_T1_2024Repository extends CrudRepository<GiaoDichHangHoa_T1_2024, Long> {
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
    List<Tuple> groupByTopSL(@Param("param") GiaoDichHangHoaReq param, Integer top);

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
    List<Tuple> groupByTopDT(@Param("param") GiaoDichHangHoaReq param, Integer top);

    @Query(value = "SELECT TOP(:top) " +
            "s.tenNhomNganhHang, s.ThuocId, " +
            "s.tenThuoc, s.tenDonVi, " +
            "(CASE WHEN s.tongNhap > 0 THEN ((s.tongBan - s.tongNhap) / s.tongNhap) * 100 ELSE NULL END)  as 'soLieuThiTruong'" +
            ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
            "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
            " FROM " +
            "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
            ", SUM(c.tongNhap) as tongNhap, SUM(c.tongBan) as tongBan " +
            "FROM GiaoDichHangHoa_T1_2024 c" +
            " WHERE 1=1 " +
            " AND (:#{#param.fromDate} IS NULL OR c.NgayGiaoDich >= :#{#param.fromDate})" +
            " AND (:#{#param.toDate} IS NULL OR c.NgayGiaoDich <= :#{#param.toDate})" +
            " AND (:#{#param.nhomDuocLyId} IS NULL OR c.nhomDuocLyId = :#{#param.nhomDuocLyId}) "+
            " AND (:#{#param.nhomNganhHangId} IS NULL OR c.nhomNganhHangId = :#{#param.nhomNganhHangId}) "+
            " AND (:#{#param.nhomHoatChatId} IS NULL OR c.nhomHoatChatId = :#{#param.nhomHoatChatId}) " +
            " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
            " ORDER BY (CASE WHEN s.tongNhap > 0 THEN ((s.tongBan - s.tongNhap) / s.tongNhap) * 100 ELSE NULL END) desc"
            , nativeQuery = true
    )
    List<Tuple> groupByTopTSLN(@Param("param") GiaoDichHangHoaReq param, Integer top);
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
    List<GiaoDichHangHoa_T1_2024> searchList(@Param("param") GiaoDichHangHoaReq param);
}
