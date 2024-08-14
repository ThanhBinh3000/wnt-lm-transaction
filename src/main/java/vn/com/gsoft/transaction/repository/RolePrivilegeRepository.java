package vn.com.gsoft.transaction.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import vn.com.gsoft.transaction.entity.RolePrivilege;
import vn.com.gsoft.transaction.model.dto.RolePrivilegeReq;

import java.util.List;

@Repository
public interface RolePrivilegeRepository extends BaseRepository<RolePrivilege, RolePrivilegeReq, Long> {
    @Query("SELECT c FROM RolePrivilege c " +
            "WHERE 1=1 "
            + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
            + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
            + " AND (:#{#param.roleId} IS NULL OR c.roleId = :#{#param.roleId}) "
            + " AND (:#{#param.privilegeId} IS NULL OR c.id = :#{#param.privilegeId}) "
            + " ORDER BY c.id desc"
    )
    Page<RolePrivilege> searchPage(@Param("param") RolePrivilegeReq param, Pageable pageable);


    @Query("SELECT c FROM RolePrivilege c " +
            "WHERE 1=1 "
            + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
            + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
            + " AND (:#{#param.roleId} IS NULL OR c.roleId = :#{#param.roleId}) "
            + " AND (:#{#param.privilegeId} IS NULL OR c.id = :#{#param.privilegeId}) "
            + " ORDER BY c.id desc"
    )
    List<RolePrivilege> searchList(@Param("param") RolePrivilegeReq param);
}
