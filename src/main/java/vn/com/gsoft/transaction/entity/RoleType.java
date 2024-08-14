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
@Table(name = "RoleType")
public class RoleType extends BaseEntity {
    @Id
    @Column(name = "ID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "RoleId")
    private Long roleId;
    @Column(name = "RoleName")
    private String roleName;
    @Column(name = "Descripition")
    private String descripition;

}
