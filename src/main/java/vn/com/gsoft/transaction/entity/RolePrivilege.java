package vn.com.gsoft.transaction.entity;

import jakarta.persistence.*;
import jakarta.persistence.Entity;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "RolePrivilege")
public class RolePrivilege extends BaseEntity{
    @Id
    @Column(name = "ID")
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    @Column(name = "RoleId")
    private Long roleId;

    @Column(name = "PrivilegeId")
    private Long privilegeId;
}
