package vn.com.gsoft.transaction.service.impl;


import lombok.extern.log4j.Log4j2;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.RecordStatusContains;
import vn.com.gsoft.transaction.constant.StatusConstant;
import vn.com.gsoft.transaction.entity.*;
import vn.com.gsoft.transaction.model.dto.*;
import vn.com.gsoft.transaction.model.system.Profile;
import vn.com.gsoft.transaction.repository.*;
import vn.com.gsoft.transaction.service.NhaThuocsService;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
@Log4j2
public class NhaThuocsServiceImpl extends BaseServiceImpl<NhaThuocs, NhaThuocsReq, Long> implements NhaThuocsService {


    private NhaThuocsRepository hdrRepo;

    @Autowired
    private TinhThanhsRepository tinhThanhsRepository;

    @Autowired
    private UserProfileRepository userProfileRepository;
    @Autowired
    private EntityRepository entityRepository;
    @Autowired
    private LichSuCapNhatThanhVienRepository lichSuCapNhatThanhVienRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    public NhaThuocsServiceImpl(NhaThuocsRepository hdrRepo, UserProfileRepository userProfileRepository,
                                EntityRepository entityRepository, TinhThanhsRepository tinhThanhsRepository,
                                LichSuCapNhatThanhVienRepository lichSuCapNhatThanhVienRepository
                                ) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
        this.userProfileRepository = userProfileRepository;
        this.entityRepository = entityRepository;
        this.tinhThanhsRepository = tinhThanhsRepository;
        this.lichSuCapNhatThanhVienRepository = lichSuCapNhatThanhVienRepository;
    }

    @Override
    public Page<NhaThuocs> searchPage(NhaThuocsReq req) throws Exception {
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        Pageable pageable = PageRequest.of(req.getPaggingReq().getPage(), req.getPaggingReq().getLimit());
        Page<NhaThuocs> nhaThuocs = hdrRepo.searchPage(req, pageable);
        nhaThuocs.getContent().forEach(item->{
          Optional<Entity> entity = entityRepository.findById(item.getEntityId());
            entity.ifPresent(value -> item.setLevel(value.getName()));
        });
        return nhaThuocs;
    }

    @Override
    public  List<Entity> searchListEntity(EntityReq rq) throws Exception {
        return entityRepository.searchList(rq);
    }
    @Override
    public List<LichSuCapNhatThanhVien> searchLichSuCapNhatThanhVien(String maThanhVien) throws Exception {
        var list = lichSuCapNhatThanhVienRepository.findLichSuCapNhatThanhVienByMaThanhVien(maThanhVien);
        if(!list.stream().isParallel()){
            list.forEach(item->{
                switch (item.getStatusId()){
                    case StatusConstant.ADD -> {
                        item.setStatusName("Tạo mới");
                        break;
                    }
                    case StatusConstant.UPDATE -> {
                        item.setStatusName("Cập nhật");
                        break;
                    }
                    case StatusConstant.DELETE -> {
                        item.setStatusName("Xóa khỏi thành viên");
                        break;
                    }
                }
            });
        }
        return list;
    }
    @Override
    public NhaThuocs create(NhaThuocsReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        if(req.getTenNhaThuoc() == null || req.getTenNhaThuoc().isEmpty()){
            throw new Exception("Tên thành viên không được null.");
        }
        if(req.getDiaChi() == null || req.getDiaChi().isEmpty()){
            throw new Exception("Địa chỉ thành viên không được null.");
        }
        if(req.getEntityId() == null){
            throw new Exception("Cấp thành viên không được null.");
        }
        if(req.getUserName() == null || req.getUserName().isEmpty()){
            throw new Exception("Tài khoản thành viên không được null.");
        }
        if(req.getPassword() == null || req.getPassword().isEmpty()){
            throw new Exception("Mật khẩu thành viên không được null.");
        }
        if(req.getDienThoai() == null || req.getDienThoai().isEmpty()){
            throw new Exception("Số điện thoại thành viên không được null.");
        }
        //kiểm tra xem tên tài khoản tv đã tồn tại chưa
        var user= userProfileRepository.findByUserNameAndHoatDong(req.getUserName(), true);
        if(user.isPresent()){
            throw new Exception("Tên tài khoản dùng để đăng nhập đã tồn tại.");
        }
        NhaThuocs nhaThuoc = new NhaThuocs();
        nhaThuoc.setMaNhaThuoc("TVM");
        nhaThuoc.setCreated(new Date());
        nhaThuoc.setCreatedByUserId(userInfo.getId());
        nhaThuoc.setRecordStatusId(RecordStatusContains.ACTIVE);
        BeanUtils.copyProperties(req, nhaThuoc, "maNhaThuoc","created", "createdByUserId", "recordStatusId");
        hdrRepo.save(nhaThuoc);
        //lưu tài khoản
        UserProfile userProfile = new UserProfile();
        userProfile.setMaNhaThuoc("TVM" + nhaThuoc.getId());
        userProfile.setUserName(req.getUserName());
        userProfile.setPassword(passwordEncoder.encode(req.getPassword()));
        userProfile.setTenDayDu(req.getTenNhaThuoc());
        userProfile.setCreated(new Date());
        userProfile.setCreatedByUserId(userInfo.getId());
        userProfile.setHoatDong(true);
        userProfile.setUserId(0L);
        userProfile.setCityId(0L);
        userProfile.setRegionId(0L);
        userProfile.setWardId(0L);
        userProfileRepository.save(userProfile);
        //lưu lịch sử
        LichSuCapNhatThanhVien lichSuCapNhatThanhVien = new LichSuCapNhatThanhVien();
        lichSuCapNhatThanhVien.setStatusId(StatusConstant.ADD);
        lichSuCapNhatThanhVien.setMaThanhVien(userProfile.getMaNhaThuoc());
        lichSuCapNhatThanhVien.setGhiChu(req.getDescription());
        lichSuCapNhatThanhVien.setNgayCapNhat(new Date());
        lichSuCapNhatThanhVienRepository.save(lichSuCapNhatThanhVien);
        return nhaThuoc;
    }
    @Override
    public NhaThuocs update(NhaThuocsReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        if(req.getTenNhaThuoc() == null || req.getTenNhaThuoc().isEmpty()){
            throw new Exception("Tên thành viên không được null.");
        }
        if(req.getMaNhaThuoc() == null || req.getMaNhaThuoc().isEmpty()){
            throw new Exception("Mã thành viên không được null.");
        }
        if(req.getEntityId() == null){
            throw new Exception("Cấp thành viên không được null.");
        }

        NhaThuocs nhaThuoc = hdrRepo.findByMaNhaThuoc(req.getMaNhaThuoc());

        nhaThuoc.setTenNhaThuoc(req.getTenNhaThuoc());
        nhaThuoc.setEntityId(req.getEntityId());
        nhaThuoc.setModified(new Date());
        nhaThuoc.setModifiedByUserId(userInfo.getId());
        hdrRepo.save(nhaThuoc);
        //lưu lịch sử
        LichSuCapNhatThanhVien lichSuCapNhatThanhVien = new LichSuCapNhatThanhVien();
        lichSuCapNhatThanhVien.setStatusId(StatusConstant.UPDATE);
        lichSuCapNhatThanhVien.setMaThanhVien(req.getMaNhaThuoc());
        lichSuCapNhatThanhVien.setGhiChu(req.getDescription());
        lichSuCapNhatThanhVien.setNgayCapNhat(new Date());
        lichSuCapNhatThanhVienRepository.save(lichSuCapNhatThanhVien);
        return nhaThuoc;
    }

    @Override
    public Boolean deleteByMaNhaThuoc(NhaThuocsReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        NhaThuocs nhaThuoc = hdrRepo.findByMaNhaThuoc(req.getMaNhaThuoc());
        nhaThuoc.setRecordStatusId(RecordStatusContains.DELETED);
        nhaThuoc.setModified(new Date());
        nhaThuoc.setModifiedByUserId(userInfo.getId());
        hdrRepo.save(nhaThuoc);
        //lưu lịch sử
        LichSuCapNhatThanhVien lichSuCapNhatThanhVien = new LichSuCapNhatThanhVien();
        lichSuCapNhatThanhVien.setStatusId(StatusConstant.DELETE);
        lichSuCapNhatThanhVien.setMaThanhVien(req.getMaNhaThuoc());
        lichSuCapNhatThanhVien.setGhiChu(req.getDescription());
        lichSuCapNhatThanhVien.setNgayCapNhat(new Date());
        lichSuCapNhatThanhVienRepository.save(lichSuCapNhatThanhVien);
        return true;
    }
}
