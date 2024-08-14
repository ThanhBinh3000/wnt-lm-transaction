package vn.com.gsoft.transaction.entity;

import jakarta.persistence.*;
import jakarta.persistence.Entity;
import lombok.*;

@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "Role")
public class Role extends BaseEntity {
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;
    @Column(name = "RoleName")
    private String roleName;
    @Column(name = "IsDeleted")
    private Boolean isDeleted;
    @Column(name = "MaNhaThuoc")
    private String maNhaThuoc;
    @Column(name = "Description")
    private String description;
    @Column(name = "Type")
    private Integer type;
    @Column(name = "IsDefault")
    private Boolean isDefault;  // true là mặc định
}

