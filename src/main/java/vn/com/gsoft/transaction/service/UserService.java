package vn.com.gsoft.transaction.service;

import vn.com.gsoft.transaction.model.system.Profile;

import java.util.Optional;

public interface UserService {
    Optional<Profile> findUserByToken(String token);
}
