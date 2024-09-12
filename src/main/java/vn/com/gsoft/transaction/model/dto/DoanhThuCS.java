package vn.com.gsoft.transaction.model.dto;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

@Data
public class DoanhThuCS implements Serializable {
    private Long thuocId;
    private BigDecimal soLuong;
    private BigDecimal nhap;
    private BigDecimal ban;
}
