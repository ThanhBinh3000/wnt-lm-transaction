package vn.com.gsoft.transaction.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;


@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "TinhThanhs")
public class TinhThanhs extends BaseEntity {
    @Id
    @Column(name = "id")
    private Long id;
    @Column(name = "MaTinhThanh")
    private String maTinhThanh;
    @Column(name = "TenTinhThanh")
    private String tenTinhThanh;
}

