package vn.com.gsoft.transaction.service.impl;


import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.LimitPageConstant;
import vn.com.gsoft.transaction.entity.*;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaRes;
import vn.com.gsoft.transaction.model.dto.TopMatHangRes;
import vn.com.gsoft.transaction.model.system.Profile;
import vn.com.gsoft.transaction.repository.*;
import vn.com.gsoft.transaction.service.GiaoDichHangHoaService;
import vn.com.gsoft.transaction.service.RedisListService;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
    public List<TopMatHangRes> topDoanhThuBanChay(GiaoDichHangHoaReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        var list = redisListService.getGiaoDichHangHoaValues(req);

        var items = list.stream()
                .map(element->(GiaoDichHangHoa) element)
                .collect(Collectors.toList());

        List<TopMatHangRes> data = items.stream()
                .collect(Collectors.groupingBy(
                        GiaoDichHangHoa::getThuocId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                x -> {
                                    if (x.isEmpty()) {
                                        return null;
                                    }
                                    var duLieuCoSo =  x.stream().filter(item->item.getMaCoSo().equals(userInfo.getMaCoSo()));
                                    BigDecimal doanhSoCoSo = BigDecimal.valueOf(0);

                                    if(duLieuCoSo.isParallel()){
                                        doanhSoCoSo = duLieuCoSo.map(item-> item.getGiaBan().multiply(item.getSoLuong())).reduce(BigDecimal.ZERO, BigDecimal::add);
                                    }
                                    return new TopMatHangRes(
                                            x.get(0).getTenThuoc(),
                                            x.get(0).getTenNhomThuoc(),
                                            x.get(0).getTenDonVi(),
                                            x.stream().map(item->item.getGiaBan().multiply(item.getSoLuong())).reduce(BigDecimal.ZERO, BigDecimal::add),
                                            doanhSoCoSo
                                    );
                                }
                        )
                ))
                .values().stream()
                .sorted((g1, g2) -> g2.getSoLieuThiTruong().compareTo(g1.getSoLieuThiTruong()))
                //.limit(req.getPageSize())
                .collect(Collectors.toList());
        return data;
    }

    @Override
    public List<TopMatHangRes> topSoLuongBanChay(GiaoDichHangHoaReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        var list = redisListService.getGiaoDichHangHoaValues(req);

        var items = list.stream()
                .map(element->(GiaoDichHangHoa) element)
                .collect(Collectors.toList());

        List<TopMatHangRes> data = items.stream()
                .collect(Collectors.groupingBy(
                        GiaoDichHangHoa::getThuocId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                x -> {
                                    if (x.isEmpty()) {
                                        return null;
                                    }
                                    var duLieuCoSo =  x.stream().filter(item->item.getMaCoSo().equals(userInfo.getMaCoSo()));
                                    BigDecimal slSoCoSo = BigDecimal.valueOf(0);

                                    if(duLieuCoSo.isParallel()){
                                        slSoCoSo = duLieuCoSo.map(item-> item.getSoLuong()).reduce(BigDecimal.ZERO, BigDecimal::add);
                                    }
                                    return new TopMatHangRes(
                                            x.get(0).getTenThuoc(),
                                            x.get(0).getTenNhomThuoc(),
                                            x.get(0).getTenDonVi(),
                                            x.stream().map(item->item.getSoLuong()).reduce(BigDecimal.ZERO, BigDecimal::add),
                                            slSoCoSo
                                    );
                                }
                        )
                ))
                .values().stream()
                .sorted((g1, g2) -> g2.getSoLieuThiTruong().compareTo(g1.getSoLieuThiTruong()))
                //.limit(req.getPageSize())
                .collect(Collectors.toList());
        return data;
    }

    @Override
    public List<TopMatHangRes> topTSLNCaoNhat(GiaoDichHangHoaReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        var list = redisListService.getGiaoDichHangHoaValues(req);

        var items = list.stream()
                .map(element->(GiaoDichHangHoa) element)
                .collect(Collectors.toList());

        List<TopMatHangRes> data = items.stream()
                .collect(Collectors.groupingBy(
                        GiaoDichHangHoa::getThuocId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                x -> {
                                    if (x.isEmpty()) {
                                        return null;
                                    }
                                    var duLieuCoSo =  x.stream().filter(item->item.getMaCoSo().equals(userInfo.getMaCoSo()));
                                    BigDecimal tslnCoSo = BigDecimal.valueOf(0);

                                    if(duLieuCoSo.isParallel()){
                                        var tongGB = duLieuCoSo.map(item-> item.getGiaBan()).reduce(BigDecimal.ZERO, BigDecimal::add);
                                        var tongGN = duLieuCoSo.map(item-> item.getGiaNhap()).reduce(BigDecimal.ZERO, BigDecimal::add);
                                        tslnCoSo = tongGN.compareTo(BigDecimal.ZERO) > 0
                                                ? ((tongGB.subtract(tongGN)).divide(tongGN, 2, RoundingMode.HALF_UP)).multiply(BigDecimal.valueOf(100))
                                                : BigDecimal.ZERO;
                                    }
                                    var tongGNTT  = x.stream().map(item->item.getGiaNhap()).reduce(BigDecimal.ZERO, BigDecimal::add);
                                    var tongGBTT  = x.stream().map(item->item.getGiaBan()).reduce(BigDecimal.ZERO, BigDecimal::add);
                                    var tslnTT  = tongGNTT.compareTo(BigDecimal.ZERO) > 0
                                            ? ((tongGBTT.subtract(tongGNTT)).divide(tongGNTT, 2, RoundingMode.HALF_UP)).multiply(BigDecimal.valueOf(100))
                                            : BigDecimal.ZERO;
                                    return new TopMatHangRes(
                                            x.get(0).getTenThuoc(),
                                            x.get(0).getTenNhomThuoc(),
                                            x.get(0).getTenDonVi(),
                                            tslnTT,
                                            tslnCoSo
                                    );
                                }
                        )
                ))
                .values().stream()
                .sorted((g1, g2) -> g2.getSoLieuThiTruong().compareTo(g1.getSoLieuThiTruong()))
                //.limit(req.getPageSize())
                .collect(Collectors.toList());
        return data;
    }

    private void pushData(){
        //        Pageable pageable = PageRequest.of(req.getPaggingReq().getPage(), req.getPaggingReq().getLimit());
//        Page<GiaoDichHangHoa> giaoDichHangHoas = hdrRepo.searchPage(req, pageable);
//        redisListService.pushDataRedis(giaoDichHangHoas.stream().toList());
    }
}
