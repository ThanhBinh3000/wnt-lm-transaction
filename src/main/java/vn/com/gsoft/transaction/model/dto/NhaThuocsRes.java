package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class NhaThuocsRes {
    private Long id;
    private String maNhaThuoc;
    private String tenNhaThuoc;
    private String nguoiPhuTrach;

    public NhaThuocsRes(Long id, String maNhaThuoc, String tenNhaThuoc, String nguoiPhuTrach) {
        this.id = id;
        this.maNhaThuoc = maNhaThuoc;
        this.tenNhaThuoc = tenNhaThuoc;
        this.nguoiPhuTrach = nguoiPhuTrach;
    }
}
