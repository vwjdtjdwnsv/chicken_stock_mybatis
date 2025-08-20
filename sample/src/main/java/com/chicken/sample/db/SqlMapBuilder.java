package com.chicken.sample.db;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;

import com.ngcas.pvl.common.ErrorCode;
import com.ngcas.pvl.common.GeneralException;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SQL 맵 빌더 클래스
 */
public class SqlMapBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SqlMapBuilder.class);
    
    private final SqlSessionFactory primarySqlSessionFactory;
    private final SqlSessionFactory readonlySqlSessionFactory;
    
    /**
     * 생성자
     * @param primarySqlSessionFactory 기본 SQL 세션 팩토리
     * @param readonlySqlSessionFactory 읽기 전용 SQL 세션 팩토리
     */    
    public SqlMapBuilder(
            @Qualifier("primarySqlSessionFactory") SqlSessionFactory primarySqlSessionFactory,
            @Qualifier("readonlySqlSessionFactory") SqlSessionFactory readonlySqlSessionFactory) {
        this.primarySqlSessionFactory = primarySqlSessionFactory;
        this.readonlySqlSessionFactory = readonlySqlSessionFactory;
    }

    // 추가: primarySqlSessionFactory getter 메서드
    public SqlSessionFactory getPrimarySqlSessionFactory() {
        return primarySqlSessionFactory;
    }
    
    // 추가: readonlySqlSessionFactory getter 메서드
    public SqlSessionFactory getReadonlySqlSessionFactory() {
        return readonlySqlSessionFactory;
    }
    
    /**
     * 데이터 삽입
     * @param statement SQL 문장
     * @param parameter 파라미터
     * @return 결과 문자열
     */
    public int insert(String statement, Map<String, String> parameter) {
        try (SqlSession session = primarySqlSessionFactory.openSession()) {
            int result = session.insert(statement, parameter);
            session.commit();
            return result;
        } catch (Exception e) {
            logger.error("Error executing insert: {}", e.getMessage(), e);
            
            // Failover 관련 에러 체크
            if (isFailoverRelatedError(e)) {
                logger.warn("Failover related error detected during insert, attempting recovery...");
                handleFailoverRecovery();
            }
            
            handleSqlException(e);
            return -1;
        }
    }
    
    /**
     * 데이터 수정
     * @param statement SQL 문장
     * @param parameter 파라미터
     * @return 결과 문자열
     */
    public int update(String statement, Map<String, String> parameter) {
        try (SqlSession session = primarySqlSessionFactory.openSession()) {
            int result = session.update(statement, parameter);
            session.commit();
            return result;
        } catch (Exception e) {
            logger.error("Error executing update: {}", e.getMessage(), e);
            
            // Failover 관련 에러 체크
            if (isFailoverRelatedError(e)) {
                logger.warn("Failover related error detected during update, attempting recovery...");
                handleFailoverRecovery();
            }
            
            handleSqlException(e);
            return -1;
        }
    }
    
    /**
     * 데이터 삭제
     * @param statement SQL 문장
     * @param parameter 파라미터
     * @return 결과 문자열
     */
    public int delete(String statement, Map<String, String> parameter) {
        try (SqlSession session = primarySqlSessionFactory.openSession()) {
            int result = session.delete(statement, parameter);
            session.commit();
            return result;
        } catch (Exception e) {
            logger.error("Error executing delete: {}", e.getMessage(), e);
            
            // Failover 관련 에러 체크
            if (isFailoverRelatedError(e)) {
                logger.warn("Failover related error detected during delete, attempting recovery...");
                handleFailoverRecovery();
            }
            
            handleSqlException(e);
            return -1;
        }
    }
    
    /**
     * 데이터 조회
     * @param statement SQL 문장
     * @param parameter 파라미터
     * @return 결과 객체
     */
    public Object select(String statement, Object parameter) {
        try (SqlSession session = readonlySqlSessionFactory.openSession()) {
            return session.selectOne(statement, parameter);
        } catch (Exception e) {
            logger.error("Error executing select: {}", e.getMessage(), e);
            
            // Failover 관련 에러 체크
            if (isFailoverRelatedError(e)) {
                logger.warn("Failover related error detected during select, attempting recovery...");
                handleFailoverRecovery();
            }
            
            handleSqlException(e);
            return null;
        }
    }
    
    /**
     * 데이터 목록 조회
     * @param statement SQL 문장
     * @param parameter 파라미터
     * @return 결과 목록
     */
    public Object selectList(String statement, Object parameter) {
        try (SqlSession session = readonlySqlSessionFactory.openSession()) {
            return session.selectList(statement, parameter);
        } catch (Exception e) {
            logger.error("Error executing selectList: {}", e.getMessage(), e);
            
            // Failover 관련 에러 체크
            if (isFailoverRelatedError(e)) {
                logger.warn("Failover related error detected during selectList, attempting recovery...");
                handleFailoverRecovery();
            }
            
            handleSqlException(e);
            return null;
        }
    }
    

    /**
     * Failover 관련 에러처리 start
     */

     private static final AtomicBoolean recoveryRunning = new AtomicBoolean(false);

    /**
     * Failover 관련 에러인지 확인
     * @param e 예외
     * @return failover 관련 에러 여부
     */
    private boolean isFailoverRelatedError(Exception e) {
        String errorMessage = e.getMessage();
        if (errorMessage == null) return false;
        
        return errorMessage.contains("read-only") ||
               errorMessage.contains("No operations allowed after connection closed") ||
               errorMessage.contains("Communications link failure") ||
               errorMessage.contains("Connection is closed") ||
               errorMessage.contains("failover") ||
               errorMessage.contains("cluster") ||
               errorMessage.contains("Connection refused") ||
               errorMessage.contains("Connection timeout");
    }
    
    /**
     * Failover 복구 처리
     */

    public void handleFailoverRecovery() {
        if (recoveryRunning.compareAndSet(false, true)) {
            try {
                logger.info("Handling failover recovery...");
                
                Thread.sleep(1000);
                
                refreshHikariConnectionPools();
                
                Thread.sleep(2000);
                
            } catch (Exception e) {
                logger.error("Failover recovery failed: {}", e.getMessage(), e);
            } finally {
                recoveryRunning.set(false);
            }
        } else {
            logger.warn("Failover recovery already in progress, skipping");
        }
    }
    
    /**
     * HikariCP Connection Pool 갱신 (failover 대응)
     */
    private void refreshHikariConnectionPools() throws Exception {
        try {
            logger.info("Refreshing HikariCP connection pools for failover recovery...");
            
            // Primary DB HikariCP Pool 갱신
            refreshHikariPool(primarySqlSessionFactory, "PRIMARY");
            
            // Readonly DB HikariCP Pool 갱신
            refreshHikariPool(readonlySqlSessionFactory, "READONLY");
            
            logger.info("HikariCP connection pools refreshed successfully");
        } catch (Exception e) {
            logger.error("Error refreshing HikariCP connection pools: {}", e.getMessage(), e);
            throw e; 
        }
    }
    
    /**
     * 개별 HikariCP Pool 갱신
     */
    private void refreshHikariPool(SqlSessionFactory sessionFactory, String poolName) throws Exception {
        try {
            // SqlSessionFactory에서 DataSource 직접 가져오기
            javax.sql.DataSource dataSource = sessionFactory.getConfiguration().getEnvironment().getDataSource();
            
            if (dataSource.isWrapperFor(HikariDataSource.class)) {
                HikariDataSource hikariDataSource = dataSource.unwrap(HikariDataSource.class);
                
                logHikariPoolStatus(hikariDataSource, poolName);
                
                // 1. 모든 연결을 강제로 닫기
                hikariDataSource.getHikariPoolMXBean().softEvictConnections();
                logger.info("{} HikariCP pool soft eviction completed", poolName);
                
                // 2. Pool 상태 재확인
                logHikariPoolStatus(hikariDataSource, poolName);
                
                logger.info("{} HikariCP pool refresh completed", poolName);
                
            } else {
                logger.warn("{} is not using HikariCP DataSource", poolName);
            }
            
        } catch (Exception e) {
            logger.error("Error refreshing {} HikariCP pool: {}", poolName, e.getMessage());
            throw e;
        }
    }
    
    /**
     * HikariCP Pool 상태 로깅
     */
    private void logHikariPoolStatus(com.zaxxer.hikari.HikariDataSource hikariDataSource, String poolName) {
        try {
            // Pool MXBean 가져오기
            com.zaxxer.hikari.HikariPoolMXBean poolMXBean = hikariDataSource.getHikariPoolMXBean();
            
            // Pool 상태 정보 추출
            int active = poolMXBean.getActiveConnections();
            int idle = poolMXBean.getIdleConnections();
            int total = poolMXBean.getTotalConnections();
            
            logger.info("{} HikariCP Pool Status - Active: {}, Idle: {}, Total: {}", 
                      poolName, active, idle, total);
            
        } catch (Exception e) {
            logger.warn("Could not get {} HikariCP pool status: {}", poolName, e.getMessage());
        }
    }
    /**
     * Failover 관련 에러처리 close
     */


    /**
     * SQL 예외 처리
     * @param e 예외
     */
    private void handleSqlException(Exception e) {
        String errorMessage = e.getMessage();
        String errorCode = ErrorCode.DB_ETC_ERROR;
        
        if (e instanceof SQLException) {
            SQLException sqlEx = (SQLException) e;
            int vendorCode = sqlEx.getErrorCode();
            
            // 오라클 에러 코드에 따른 처리
            if (vendorCode == 942 || errorMessage.contains("ORA-00942") || errorMessage.contains("table or view does not exist")) {
                errorCode = ErrorCode.TABLE_NOT_FOUND_ERROR;
            } else if (vendorCode == 904 || errorMessage.contains("ORA-00904") || errorMessage.contains("invalid column name")) {
                errorCode = ErrorCode.INVALID_COLUMN_ERROR;
            } else if (vendorCode == 1 || errorMessage.contains("ORA-00001") || errorMessage.contains("unique constraint")) {
                errorCode = ErrorCode.UNIQUE_INDEX_ERROR;
            } else if (vendorCode == 1400 || errorMessage.contains("ORA-01400") || errorMessage.contains("cannot insert NULL")) {
                errorCode = ErrorCode.NULL_INTO_NOT_NULL_COLUMN_ERROR;
            } else if (errorMessage.contains("Connection") || errorMessage.contains("connection")) {
                errorCode = ErrorCode.DB_CONNECTION_ERROR;
            }
        }
        
        throw new GeneralException(errorCode, "Database error: " + errorMessage, e);
    }

     // DB 연결 확인 메서드 추가
     public void reConnectionCheck() {
        logger.info("Checking database connection...");
        try (SqlSession session = primarySqlSessionFactory.openSession();
            Connection conn = session.getConnection();
            Statement stmt = conn.createStatement()) {
            // session.selectOne("common.getDualValue");
            stmt.execute("SELECT 1");
            logger.info("Primary database connection is OK");
        } catch (Exception e) {
            logger.error("Primary database connection check failed: {}", e.getMessage());
        }
        
        try (SqlSession session = readonlySqlSessionFactory.openSession();
             Connection conn = session.getConnection();
            Statement stmt = conn.createStatement()) {
            // session.selectOne("common.getDualValue");
            stmt.execute("SELECT 1");
            // session.selectOne("common.getDualValue");
            logger.info("Readonly database connection is OK");
        } catch (Exception e) {
            logger.error("Readonly database connection check failed: {}", e.getMessage());
        }
    }
}

