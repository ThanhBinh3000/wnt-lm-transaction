package vn.com.gsoft.transaction.service.impl;


import com.ctc.wstx.util.DataUtil;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.BaoCaoContains;
import vn.com.gsoft.transaction.constant.LimitPageConstant;
import vn.com.gsoft.transaction.entity.*;
import vn.com.gsoft.transaction.model.dto.*;
import vn.com.gsoft.transaction.model.system.Profile;
import vn.com.gsoft.transaction.repository.*;
import vn.com.gsoft.transaction.service.GiaoDichHangHoaService;
import vn.com.gsoft.transaction.service.RedisListService;
import vn.com.gsoft.transaction.util.system.DataUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Log4j2
public class GiaoDichHangHoaServiceImpl extends BaseServiceImpl<GiaoDichHangHoa, GiaoDichHangHoaReq, Long> implements GiaoDichHangHoaService {


    private GiaoDichHangHoaRepository hdrRepo;
    @Autowired
    private RedisListService redisListService;
    @Autowired
    private HangHoaRepository hangHoaRepo;

    @Autowired
    public GiaoDichHangHoaServiceImpl(GiaoDichHangHoaRepository hdrRepo,
                                      RedisListService redisListService,
                                      HangHoaRepository hangHoaRepo
                                ) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
        this.redisListService = redisListService;
        this.hangHoaRepo = hangHoaRepo;
    }
    @Override
    public List<TopMatHangRes> topDoanhThuBanChay(GiaoDichHangHoaReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        setDefaultDates(req);
        req.setMaCoSo(userInfo.getMaCoSo());
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());

        var listData = DataUtils.convertList(hdrRepo.groupByTopDoanhThu(req, req.getPageSize()), TopMatHangRes.class);
        if(listData != null){
            listData.forEach(x->{
                var hh = hangHoaRepo.findByThuocId(x.getThuocId());
                if(hh != null){
                    x.setTenThuoc(hh.getTenThuoc());
                    x.setTenDonVi(hh.getTenDonVi());
                    x.setTenNhomNganhHang(hh.getTenNhomNganhHang());
                }
            });
        }
        return listData;
    }

    @Override
    public List<TopMatHangRes> topSLBanChay(GiaoDichHangHoaReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        setDefaultDates(req);
        req.setMaCoSo(userInfo.getMaCoSo());
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        req.setLoaiGiaoDich(2);
        List<TopMatHangRes> listData = new ArrayList<TopMatHangRes>();

//        String dateStr1 = "31/08/2024 00:00:00";
//        String dateStr2 = "01/06/2024 00:00:00";
//
//        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
//        if(dateFormat.parse(dateStr1).compareTo(req.getFromDate()) == 0 && dateFormat.parse(dateStr2).compareTo(req.getToDate()) == 0){
//            listData = redisListService.getAllDataFromRedis("top-sl-3-thang-gan-nhat").stream()
//                    .sorted((g1, g2) -> g2.getSoLieuThiTruong().compareTo(g1.getSoLieuThiTruong())).limit(req.getPageSize()).toList();
//            //lấy ra list id
//            if(userInfo.getMaCoSo() != null){
//                Object[] ids = listData.stream().map(x->x.getThuocId()).toArray();
//                var soLieuCoSo = hdrRepo.groupByTopDoanhSoLuongByMaCoSo(req, (Long[]) ids);
//            }
//
//        }else {
//            listData = DataUtils.convertList(hdrRepo.groupByTopDoanhSoLuong(req, req.getPageSize()), TopMatHangRes.class);
//            if(listData != null){
//                listData.forEach(x->{
//                    var hh = hangHoaRepo.findByThuocId(x.getThuocId());
//                    if(hh != null){
//                        x.setTenThuoc(hh.getTenThuoc());
//                        x.setTenDonVi(hh.getTenDonVi());
//                        x.setTenNhomNganhHang(hh.getTenNhomNganhHang());
//                    }
//                });
//            }
//        }

        listData = DataUtils.convertList(hdrRepo.groupByTopDoanhSoLuong(req, req.getPageSize()), TopMatHangRes.class);
            if(listData != null){
                listData.forEach(x->{
                    var hh = hangHoaRepo.findByThuocId(x.getThuocId());
                    if(hh != null){
                        x.setTenThuoc(hh.getTenThuoc());
                        x.setTenDonVi(hh.getTenDonVi());
                        x.setTenNhomNganhHang(hh.getTenNhomNganhHang());
                    }
                });
            }
        return listData;
    }

    @Override
    public List<TopMatHangRes> topTSLNCaoNhat(GiaoDichHangHoaReq req) throws Exception {
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        setDefaultDates(req);
        req.setMaCoSo(userInfo.getMaCoSo());
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());

        var listData = DataUtils.convertList(hdrRepo.groupByTopDoanhTSLN(req, req.getPageSize()), TopMatHangRes.class);
        if(listData != null){
            listData.forEach(x->{
                var hh = hangHoaRepo.findByThuocId(x.getThuocId());
                if(hh != null){
                    x.setTenThuoc(hh.getTenThuoc());
                    x.setTenDonVi(hh.getTenDonVi());
                    x.setTenNhomNganhHang(hh.getTenNhomNganhHang());
                }
                if(x.getGN().compareTo(BigDecimal.ZERO) > 0 && x.getGB() != null){
                    x.setSoLieuThiTruong((x.getGB().subtract(x.getGN()).divide(x.getGN(), 2, RoundingMode.HALF_UP)).multiply(BigDecimal.valueOf(100)));
                }
                if(x.getGNCS() != null && x.getGNCS().compareTo(BigDecimal.ZERO) > 0 && x.getGBCS() != null){
                    x.setSoLieuCoSo((x.getGBCS().subtract(x.getGNCS()).divide(x.getGNCS(), 2, RoundingMode.HALF_UP)).multiply(BigDecimal.valueOf(100)));
                }
            });
        }
        return listData;
    }

    private void setDefaultDates(GiaoDichHangHoaReq req) {
        // Tạo đối tượng Calendar để tính toán ngày mặc định
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.YEAR, -5); // Trừ 5 năm so với năm hiện tại
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        // Cài đặt ngày mặc định cho req nếu req.getFromDate() hoặc req.getToDate() là null
        if (req.getFromDate() == null) {
            req.setFromDate(calendar.getTime());
        }
        if (req.getToDate() == null) {
            req.setToDate(new Date());
        }
    }

    @Override
    public void pushData() throws Exception {
        var req = new GiaoDichHangHoaReq();
        //Calendar fdate = Calendar.getInstance();
        //fdate.add(Calendar.MONTH, -3);
        req.setLoaiGiaoDich(2);
        //req.setFromDate(fdate.getTime());
        String dateStr1 = "31/08/2024";
        String dateStr2 = "01/06/2024";

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        req.setToDate(dateFormat.parse(dateStr1));
        req.setFromDate(dateFormat.parse(dateStr2));
        req.setDongBang(false);
        var list = DataUtils.convertList(hdrRepo.groupByTopDoanhSoLuongPushRedis(req, 2000), TopMatHangRes.class);
        redisListService.pushDataToRedisByTime(list, "top-sl", "3-thang-gan-nhat");

    }
}
