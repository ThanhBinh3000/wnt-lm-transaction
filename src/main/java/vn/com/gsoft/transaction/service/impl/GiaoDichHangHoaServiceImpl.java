package vn.com.gsoft.transaction.service.impl;


import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.BaoCaoContains;
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

        setDefaultDates(req);
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        var list = redisListService.getGiaoDichHangHoaValues(req);

        var items = list.stream()
                .map(element->(GiaoDichHangHoa) element)
                .collect(Collectors.toList());
        if(req.getNhomDuocLyId() != null && req.getNhomDuocLyId() > 0){
            items = items.stream().filter(item->item.getNhomDuocLyId().equals(req.getNhomDuocLyId()))
                    .collect(Collectors.toList());
        }
        Calendar dateArchive = Calendar.getInstance();
        dateArchive.add(Calendar.YEAR, -1);
        //kiểm tra xem thời gian xem báo cáo có lớn hơn thời điểm archive không;
        if(req.getFromDate().before(dateArchive.getTime())){
            req.setToDate(dateArchive.getTime());
            var listArchive = hdrRepo.searchList(req);
            if(!listArchive.stream().isParallel()){
                items.addAll(listArchive);
            }
        }

        List<TopMatHangRes> data = new ArrayList<>();

        data = items.stream()
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

                                    if(!duLieuCoSo.isParallel()){
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
                .limit(req.getPageSize())
                .collect(Collectors.toList());
        return  data;
    }

    @Override
    public List<TopMatHangRes> topSoLuongBanChay(GiaoDichHangHoaReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        setDefaultDates(req);
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        var list = redisListService.getGiaoDichHangHoaValues(req);

        var items = list.stream()
                .map(element->(GiaoDichHangHoa) element)
                .collect(Collectors.toList());
        if(req.getNhomDuocLyId() != null && req.getNhomDuocLyId() > 0){
            items = items.stream().filter(item->item.getNhomDuocLyId().equals(req.getNhomDuocLyId()))
                    .collect(Collectors.toList());
        }

        Calendar dateArchive = Calendar.getInstance();
        dateArchive.add(Calendar.YEAR, -1);
        //kiểm tra xem thời gian xem báo cáo có lớn hơn thời điểm archive không;
        if(req.getFromDate().before(dateArchive.getTime())){
            req.setToDate(dateArchive.getTime());
            var listArchive = hdrRepo.searchList(req);
            if(!listArchive.stream().isParallel()){
                items.addAll(listArchive);
            }
        }
        List<TopMatHangRes> data = new ArrayList<>();
        data = items.stream()
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

                                    if(!duLieuCoSo.isParallel()){
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
                .limit(req.getPageSize())
                .collect(Collectors.toList());

        return data;
    }

    @Override
    public List<TopMatHangRes> topTSLNCaoNhat(GiaoDichHangHoaReq req) throws Exception {
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        setDefaultDates(req);
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        var list = redisListService.getGiaoDichHangHoaValues(req);

        var items = list.stream()
                .map(element -> (GiaoDichHangHoa) element)
                .collect(Collectors.toList());
        if(req.getNhomDuocLyId() != null && req.getNhomDuocLyId() > 0){
            items = items.stream().filter(item->item.getNhomDuocLyId().equals(req.getNhomDuocLyId()))
                    .collect(Collectors.toList());
        }

        Calendar dateArchive = Calendar.getInstance();
        dateArchive.add(Calendar.YEAR, -1);

        // Kiểm tra thời gian xem báo cáo
        if (req.getFromDate().before(dateArchive.getTime())) {
            req.setToDate(dateArchive.getTime());
            items.addAll(hdrRepo.searchList(req));
        }

        List<TopMatHangRes> data = new ArrayList<>();
        data = items.parallelStream()
                .collect(Collectors.groupingByConcurrent(
                        GiaoDichHangHoa::getThuocId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                x -> {
                                    if (x.isEmpty()) return null;

                                    var duLieuCoSo = x.parallelStream()
                                            .filter(item -> item.getMaCoSo().equals(userInfo.getMaCoSo()))
                                            .collect(Collectors.toList());

                                    BigDecimal tslnCoSo = BigDecimal.ZERO;
                                    if (!duLieuCoSo.isEmpty()) {
                                        BigDecimal tongGB = duLieuCoSo.parallelStream()
                                                .map(GiaoDichHangHoa::getGiaBan)
                                                .reduce(BigDecimal.ZERO, BigDecimal::add);

                                        BigDecimal tongGN = duLieuCoSo.parallelStream()
                                                .map(GiaoDichHangHoa::getGiaNhap)
                                                .reduce(BigDecimal.ZERO, BigDecimal::add);

                                        if (tongGN.compareTo(BigDecimal.ZERO) > 0) {
                                            tslnCoSo = (tongGB.subtract(tongGN))
                                                    .divide(tongGN, 2, RoundingMode.HALF_UP)
                                                    .multiply(BigDecimal.valueOf(100));
                                        }
                                    }

                                    BigDecimal tongGNTT = x.parallelStream()
                                            .map(GiaoDichHangHoa::getGiaNhap)
                                            .reduce(BigDecimal.ZERO, BigDecimal::add);

                                    BigDecimal tongGBTT = x.parallelStream()
                                            .map(GiaoDichHangHoa::getGiaBan)
                                            .reduce(BigDecimal.ZERO, BigDecimal::add);

                                    BigDecimal tslnTT = BigDecimal.ZERO;
                                    if (tongGNTT.compareTo(BigDecimal.ZERO) > 0) {
                                        tslnTT = (tongGBTT.subtract(tongGNTT))
                                                .divide(tongGNTT, 2, RoundingMode.HALF_UP)
                                                .multiply(BigDecimal.valueOf(100));
                                    }

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
                .values().parallelStream()
                .filter(Objects::nonNull)
                .sorted((g1, g2) -> g2.getSoLieuThiTruong().compareTo(g1.getSoLieuThiTruong()))
                .limit(req.getPageSize())
                .collect(Collectors.toList());

        return data;
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
    public void pushData(){
        var rep = new GiaoDichHangHoaReq();
        Calendar dateArchive = Calendar.getInstance();
        dateArchive.add(Calendar.YEAR, -1);
        rep.setFromDate(dateArchive.getTime());
        var list = hdrRepo.searchList(rep);
        redisListService.pushDataRedis(list);
    }
}
