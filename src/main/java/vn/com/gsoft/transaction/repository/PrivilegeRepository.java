package vn.com.gsoft.transaction.repository;

import vn.com.gsoft.transaction.entity.*;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import vn.com.gsoft.transaction.model.dto.PrivilegeReq;

import java.util.List;

@Repository
public interface PrivilegeRepository extends BaseRepository<Privilege, PrivilegeReq, Long> {
    @Query("SELECT c FROM Privilege c " +
            "WHERE 1=1 "
            + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
            + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
            + " AND (:#{#param.code} IS NULL OR lower(c.code) LIKE lower(concat('%',CONCAT(:#{#param.code},'%'))))"
            + " AND (:#{#param.parentCode} IS NULL OR lower(c.parentCode) LIKE lower(concat('%',CONCAT(:#{#param.parentCode},'%'))))"
            + " AND (:#{#param.name} IS NULL OR lower(c.name) LIKE lower(concat('%',CONCAT(:#{#param.name},'%'))))"
            + " ORDER BY c.id desc"
    )
    Page<Privilege> searchPage(@Param("param") PrivilegeReq param, Pageable pageable);


    @Query("SELECT c FROM Privilege c " +
            "WHERE 1=1 "
            + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
            + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
            + " AND (:#{#param.code} IS NULL OR lower(c.code) LIKE lower(concat('%',CONCAT(:#{#param.code},'%'))))"
            + " AND (:#{#param.parentCode} IS NULL OR lower(c.parentCode) LIKE lower(concat('%',CONCAT(:#{#param.parentCode},'%'))))"
            + " AND (:#{#param.name} IS NULL OR lower(c.name) LIKE lower(concat('%',CONCAT(:#{#param.name},'%'))))"
            + " ORDER BY c.id desc"
    )
    List<Privilege> searchList(@Param("param") PrivilegeReq param);

}
