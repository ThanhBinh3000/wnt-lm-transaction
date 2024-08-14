package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TrienKhaisRes {
    private Long id;
    private String name;
    private Long type;
    private String maNhaThuoc;
    private Boolean active;
}
