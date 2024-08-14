package vn.com.gsoft.transaction.repository;

import vn.com.gsoft.transaction.entity.*;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import vn.com.gsoft.transaction.model.dto.PrivilegeEntityReq;

import java.util.List;

@Repository
public interface PrivilegeEntityRepository extends BaseRepository<PrivilegeEntity, PrivilegeEntityReq, Long> {
  @Query("SELECT c FROM PrivilegeEntity c " +
         "WHERE 1=1 "
          + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
          + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
          + " AND (:#{#param.privilegeId} IS NULL OR c.privilegeId = :#{#param.privilegeId}) "
          + " AND (:#{#param.entityId} IS NULL OR c.entityId = :#{#param.entityId}) "
          + " ORDER BY c.id desc"
  )
  Page<PrivilegeEntity> searchPage(@Param("param") PrivilegeEntityReq param, Pageable pageable);
  
  
  @Query("SELECT c FROM PrivilegeEntity c " +
         "WHERE 1=1 "
          + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
          + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
          + " AND (:#{#param.privilegeId} IS NULL OR c.privilegeId = :#{#param.privilegeId}) "
          + " AND (:#{#param.entityId} IS NULL OR c.entityId = :#{#param.entityId}) "
          + " ORDER BY c.id desc"
  )
  List<PrivilegeEntity> searchList(@Param("param") PrivilegeEntityReq param);

}
