package vn.com.gsoft.transaction.response;

import lombok.Data;

@Data
public class BaseResponse {
	Object data;
	Object otherData;
	int status;//0: succ <>0: fail
	String message;
	Object included;
}