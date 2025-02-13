package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;

import java.time.LocalDate;

@EqualsAndHashCode(callSuper = true)
@Data
public class GiaoDichHangHoaReq extends BaseRequest {
    private Boolean dongBang;
    private Integer LoaiGiaoDich;
    private String maCoSo;
    private Integer pageSize;
    private Integer nganhHangId;
    private Integer nhomDuocLyId;
    private Integer nhomHoatChatId;
    private Integer nhomNganhHangId;
    private Long[] thuocIds;
    private Integer type ;
    private Integer[] types ;
    private LocalDate fDate;
    private LocalDate tDate;
}
