package com.chicken.sample.service;

import com.chicken.sample.db.SqlMapBuilder;
import com.chicken.sample.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User 서비스 클래스
 */
@Service
public class UserService {
    private static final Logger logger = LoggerFactory.getLogger(UserService.class);

    private final SqlMapBuilder sqlMapBuilder;

    @Autowired
    public UserService(SqlMapBuilder sqlMapBuilder) {
        this.sqlMapBuilder = sqlMapBuilder;
    }

    /**
     * User 등록
     */
    public int createUser(User user) {
        logger.info("Creating user: {}", user.getUserId());
        
        Map<String, String> params = new HashMap<>();
        params.put("userId", user.getUserId());
        params.put("name", user.getName());
        params.put("email", user.getEmail());
        params.put("phone", user.getPhone());
        params.put("status", user.getStatus());
        params.put("createdAt", LocalDateTime.now().toString());
        params.put("updatedAt", LocalDateTime.now().toString());

        int result = sqlMapBuilder.insert("user.insertUser", params);
        
        if (result > 0) {
            logger.info("User created successfully: {}", user.getUserId());
        } else {
            logger.error("Failed to create user: {}", user.getUserId());
        }
        
        return result;
    }

    /**
     * User 조회
     */
    public User getUser(String userId) {
        logger.info("Getting user: {}", userId);
        
        Map<String, String> params = new HashMap<>();
        params.put("userId", userId);

        Object result = sqlMapBuilder.select("user.selectUser", params);
        
        if (result instanceof Map) {
            Map<String, Object> row = (Map<String, Object>) result;
            return mapToUser(row);
        }
        
        return null;
    }

    /**
     * User 목록 조회
     */
    public List<User> getAllUsers() {
        logger.info("Getting all users");
        
        Object result = sqlMapBuilder.selectList("user.selectAllUsers", null);
        
        if (result instanceof List) {
            List<Map<String, Object>> rows = (List<Map<String, Object>>) result;
            return rows.stream()
                    .map(this::mapToUser)
                    .toList();
        }
        
        return List.of();
    }

    /**
     * User 수정
     */
    public int updateUser(User user) {
        logger.info("Updating user: {}", user.getUserId());
        
        Map<String, String> params = new HashMap<>();
        params.put("userId", user.getUserId());
        params.put("name", user.getName());
        params.put("email", user.getEmail());
        params.put("phone", user.getPhone());
        params.put("status", user.getStatus());
        params.put("updatedAt", LocalDateTime.now().toString());

        int result = sqlMapBuilder.update("user.updateUser", params);
        
        if (result > 0) {
            logger.info("User updated successfully: {}", user.getUserId());
        } else {
            logger.error("Failed to update user: {}", user.getUserId());
        }
        
        return result;
    }

    /**
     * User 삭제
     */
    public int deleteUser(String userId) {
        logger.info("Deleting user: {}", userId);
        
        Map<String, String> params = new HashMap<>();
        params.put("userId", userId);

        int result = sqlMapBuilder.delete("user.deleteUser", params);
        
        if (result > 0) {
            logger.info("User deleted successfully: {}", userId);
        } else {
            logger.error("Failed to delete user: {}", userId);
        }
        
        return result;
    }

    /**
     * Map을 User 객체로 변환
     */
    private User mapToUser(Map<String, Object> row) {
        User user = new User();
        
        if (row.get("ID") != null) {
            user.setId(Long.valueOf(row.get("ID").toString()));
        }
        if (row.get("USER_ID") != null) {
            user.setUserId(row.get("USER_ID").toString());
        }
        if (row.get("NAME") != null) {
            user.setName(row.get("NAME").toString());
        }
        if (row.get("EMAIL") != null) {
            user.setEmail(row.get("EMAIL").toString());
        }
        if (row.get("PHONE") != null) {
            user.setPhone(row.get("PHONE").toString());
        }
        if (row.get("STATUS") != null) {
            user.setStatus(row.get("STATUS").toString());
        }
        if (row.get("CREATED_AT") != null) {
            user.setCreatedAt(LocalDateTime.parse(row.get("CREATED_AT").toString()));
        }
        if (row.get("UPDATED_AT") != null) {
            user.setUpdatedAt(LocalDateTime.parse(row.get("UPDATED_AT").toString()));
        }
        
        return user;
    }

    /**
     * DB 연결 상태 확인
     */
    public void checkDatabaseConnection() {
        logger.info("Checking database connection...");
        sqlMapBuilder.reConnectionCheck();
    }
}