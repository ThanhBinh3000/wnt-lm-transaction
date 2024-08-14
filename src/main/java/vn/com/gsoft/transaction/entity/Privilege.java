package vn.com.gsoft.transaction.entity;

import lombok.*;

import jakarta.persistence.*;
import jakarta.persistence.Entity;

@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "Privilege")
public class Privilege extends BaseEntity{
    @Id
    @Column(name = "Id")
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;
    @Column(name = "Code")
    private String code;
    @Column(name = "ParentCode")
    private String parentCode;
    @Column(name = "Name")
    private String name;
    @Column(name = "Enable")
    private Boolean enable;
}

