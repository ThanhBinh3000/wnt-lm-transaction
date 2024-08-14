package vn.com.gsoft.transaction.service.impl;


import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.entity.*;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.repository.*;
import vn.com.gsoft.transaction.service.GiaoDichHangHoaService;
import vn.com.gsoft.transaction.service.RedisListService;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Log4j2
public class GiaoDichHangHoaServiceImpl extends BaseServiceImpl<GiaoDichHangHoa, GiaoDichHangHoaReq, Long> implements GiaoDichHangHoaService {


    private GiaoDichHangHoaRepository hdrRepo;
    @Autowired
    private RedisListService redisListService;

    @Autowired
    public GiaoDichHangHoaServiceImpl(GiaoDichHangHoaRepository hdrRepo
                                ) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
    }
    @Override
    public List<GiaoDichHangHoa> topDoanhThuBanChay(GiaoDichHangHoaReq req) throws Exception{
        //Pageable pageable = PageRequest.of(req.getPaggingReq().getPage(), req.getPaggingReq().getLimit());
        //Page<GiaoDichHangHoa> giaoDichHangHoas = hdrRepo.searchPage(req, pageable);
        //redisListService.pushDataRedis(giaoDichHangHoas.stream().toList());

        var list = redisListService.getGiaoDichHangHoaValues(req);
//        var giaoDichHangHoas = list.stream()
//                .map(element->(GiaoDichHangHoa) element)
//                .collect(Collectors.groupingBy(GiaoDichHangHoa ::getThuocId, new GiaoDichHangHoa(){
//
//                }));
        var items = list.stream()
                .map(element->(GiaoDichHangHoa) element)
                .collect(Collectors.toList());
        List<GiaoDichHangHoa> data = items.stream()
                .collect(Collectors.groupingBy(
                        GiaoDichHangHoa::getId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                x -> {
                                    if (x.isEmpty()) {
                                        return null;
                                    }
                                    return new GiaoDichHangHoa(
                                            x.get(0).getTenThuoc(),
                                            x.get(0).getTenNhomThuoc(),
                                            x.get(0).getTenDonVi(),
                                            x.stream().map(item -> item.getGiaBan().multiply(item.getSoLuong()))
                                                    .reduce(BigDecimal.ZERO, BigDecimal::add)
                                    );
                                }
                        )
                ))
                .values().stream()
                .collect(Collectors.toList()); // Chuyển đổi kết quả thành danh sách
        return data;
    }

    public void addDataRedis(){
        GiaoDichHangHoaReq rep = new GiaoDichHangHoaReq();
        rep.setToDate(new Date());
    }
}
