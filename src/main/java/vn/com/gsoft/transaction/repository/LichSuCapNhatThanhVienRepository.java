package vn.com.gsoft.transaction.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import vn.com.gsoft.transaction.entity.LichSuCapNhatThanhVien;

import java.util.List;

public interface LichSuCapNhatThanhVienRepository extends CrudRepository<LichSuCapNhatThanhVien, Integer> {
    List<LichSuCapNhatThanhVien> findLichSuCapNhatThanhVienByMaThanhVien(@Param("maThanhVien") String maThanhVien);
}
