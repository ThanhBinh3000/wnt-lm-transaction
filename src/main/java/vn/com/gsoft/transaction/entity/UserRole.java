package vn.com.gsoft.transaction.entity;

import jakarta.persistence.*;
import jakarta.persistence.Entity;
import lombok.*;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "UserRole")
@EntityListeners(AuditingEntityListener.class)
public class UserRole extends BaseEntity{
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;
    private Long roleId;
    private Long userId;
}
