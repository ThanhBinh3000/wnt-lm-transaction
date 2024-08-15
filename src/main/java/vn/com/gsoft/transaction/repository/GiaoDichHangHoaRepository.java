package vn.com.gsoft.transaction.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;

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
            + " ORDER BY c.NgayGiaoDich desc", nativeQuery = true
    )
    List<GiaoDichHangHoa> searchList(@Param("param") GiaoDichHangHoaReq param);

}
