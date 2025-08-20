package com.chicken.sample.controller;

import com.chicken.sample.entity.User;
import com.chicken.sample.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User 컨트롤러 클래스
 */
@RestController
@RequestMapping("/api/users")
public class UserController {
    private static final Logger logger = LoggerFactory.getLogger(UserController.class);

    private final UserService userService;

    @Autowired
    public UserController(UserService userService) {
        this.userService = userService;
    }

    /**
     * User 등록
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createUser(@RequestBody User user) {
        logger.info("Received request to create user: {}", user.getUserId());
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            int result = userService.createUser(user);
            
            if (result > 0) {
                response.put("success", true);
                response.put("message", "User created successfully");
                response.put("data", user);
                return ResponseEntity.status(HttpStatus.CREATED).body(response);
            } else {
                response.put("success", false);
                response.put("message", "Failed to create user");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
        } catch (Exception e) {
            logger.error("Error creating user: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("message", "Error creating user: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * User 조회
     */
    @GetMapping("/{userId}")
    public ResponseEntity<Map<String, Object>> getUser(@PathVariable String userId) {
        logger.info("Received request to get user: {}", userId);
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            User user = userService.getUser(userId);
            
            if (user != null) {
                response.put("success", true);
                response.put("message", "User retrieved successfully");
                response.put("data", user);
                return ResponseEntity.ok(response);
            } else {
                response.put("success", false);
                response.put("message", "User not found: " + userId);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }
        } catch (Exception e) {
            logger.error("Error getting user: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("message", "Error getting user: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * User 목록 조회
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllUsers() {
        logger.info("Received request to get all users");
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            List<User> users = userService.getAllUsers();
            
            response.put("success", true);
            response.put("message", "Users retrieved successfully");
            response.put("data", users);
            response.put("count", users.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error getting all users: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("message", "Error getting all users: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * User 수정
     */
    @PutMapping("/{userId}")
    public ResponseEntity<Map<String, Object>> updateUser(
            @PathVariable String userId, 
            @RequestBody User user) {
        logger.info("Received request to update user: {}", userId);
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            // userId를 path variable에서 설정
            user.setUserId(userId);
            
            int result = userService.updateUser(user);
            
            if (result > 0) {
                response.put("success", true);
                response.put("message", "User updated successfully");
                response.put("data", user);
                return ResponseEntity.ok(response);
            } else {
                response.put("success", false);
                response.put("message", "Failed to update user or user not found");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
        } catch (Exception e) {
            logger.error("Error updating user: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("message", "Error updating user: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * User 삭제
     */
    @DeleteMapping("/{userId}")
    public ResponseEntity<Map<String, Object>> deleteUser(@PathVariable String userId) {
        logger.info("Received request to delete user: {}", userId);
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            int result = userService.deleteUser(userId);
            
            if (result > 0) {
                response.put("success", true);
                response.put("message", "User deleted successfully");
                return ResponseEntity.ok(response);
            } else {
                response.put("success", false);
                response.put("message", "Failed to delete user or user not found");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
        } catch (Exception e) {
            logger.error("Error deleting user: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("message", "Error deleting user: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * DB 연결 상태 확인
     */
    @GetMapping("/health/db")
    public ResponseEntity<Map<String, Object>> checkDatabaseHealth() {
        logger.info("Received request to check database health");
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            userService.checkDatabaseConnection();
            response.put("success", true);
            response.put("message", "Database connection is healthy");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Database health check failed: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("message", "Database health check failed: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
        }
    }

    /**
     * 헬스체크
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "User Management Service is running");
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
    }
}