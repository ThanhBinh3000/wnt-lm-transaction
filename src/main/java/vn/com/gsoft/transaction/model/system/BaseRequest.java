package vn.com.gsoft.transaction.model.system;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class BaseRequest {
	private Long id;
	private String maNhaThuoc;
	private Long userIdQueryData;
	private String textSearch;
	private Long recordStatusId;
	private List<Long> recordStatusIds;
	private PaggingReq paggingReq;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
	private Date fromDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
	private Date toDate;
}