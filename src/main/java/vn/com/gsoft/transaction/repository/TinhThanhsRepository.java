package vn.com.gsoft.transaction.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import vn.com.gsoft.transaction.entity.TinhThanhs;
import vn.com.gsoft.transaction.model.dto.TinhThanhsReq;

import java.util.List;

@Repository
public interface TinhThanhsRepository extends BaseRepository<TinhThanhs, TinhThanhsReq, Long> {
  @Query("SELECT c FROM TinhThanhs c " +
         "WHERE 1=1 "
          + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
          + " AND (:#{#param.maTinhThanh} IS NULL OR lower(c.maTinhThanh) LIKE lower(concat('%',CONCAT(:#{#param.maTinhThanh},'%'))))"
          + " AND (:#{#param.tenTinhThanh} IS NULL OR lower(c.tenTinhThanh) LIKE lower(concat('%',CONCAT(:#{#param.tenTinhThanh},'%'))))"
          + " ORDER BY c.tenTinhThanh asc"
  )
  Page<TinhThanhs> searchPage(@Param("param") TinhThanhsReq param, Pageable pageable);
  
  
  @Query("SELECT c FROM TinhThanhs c " +
         "WHERE 1=1 "
          + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
          + " AND (:#{#param.maTinhThanh} IS NULL OR lower(c.maTinhThanh) LIKE lower(concat('%',CONCAT(:#{#param.maTinhThanh},'%'))))"
          + " AND (:#{#param.tenTinhThanh} IS NULL OR lower(c.tenTinhThanh) LIKE lower(concat('%',CONCAT(:#{#param.tenTinhThanh},'%'))))"
          + " ORDER BY c.tenTinhThanh asc"
  )
  List<TinhThanhs> searchList(@Param("param") TinhThanhsReq param);

}
