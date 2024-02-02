-- Benchmark Productivity

DELIMITER $$

CREATE PROCEDURE benchmark_productivity(IN period_param BIGINT) 
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE db_name VARCHAR(255);
    DECLARE cur CURSOR FOR 
        SELECT SCHEMA_NAME 
        FROM information_schema.SCHEMATA 
        WHERE 
			SCHEMA_NAME NOT IN ('mysql', 'information_schema','performance_schema','sys','workflow','master','benchmark')
			AND SCHEMA_NAME NOT IN (SELECT tenant FROM ignore_tenant);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Check if period_param is NULL and set it to the current year and month
    IF period_param IS NULL THEN
		SET period_param = EXTRACT(YEAR FROM DATE_SUB(NOW(), INTERVAL 1 MONTH)) * 100 +
                                 EXTRACT(MONTH FROM DATE_SUB(NOW(), INTERVAL 1 MONTH));        
    END IF;

    SET @sql = '';

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO db_name;
        IF done THEN
            LEAVE read_loop;
        END IF;
        SET @sql = CONCAT(@sql, 'SELECT ', period_param, ' AS `period`, estimated_contribution, expected_contribution, author_coding_time_percentage, productivity_score FROM `', db_name, '`.cooked_data_productivity WHERE aggregate_type = 16 AND version_from <= ', period_param, ' AND estimated_contribution BETWEEN 2 AND 30 UNION ALL ');
    END LOOP;

    CLOSE cur;

    -- Remove the trailing 'UNION ALL'
    SET @sql = SUBSTRING(@sql, 1, CHAR_LENGTH(@sql) - 10);

    SET @sql = CONCAT('SELECT `period`, AVG(estimated_contribution) AS estimated_contribution, AVG(expected_contribution) AS expected_contribution, AVG(author_coding_time_percentage) AS author_coding_time_percentage, SUM(estimated_contribution) / SUM(2 * expected_contribution) AS productivity_score FROM (', @sql, ') AS t GROUP BY `period`');

    SET @insert_sql = CONCAT('INSERT INTO productivity (`period`, estimated_contribution, expected_contribution, author_coding_time_percentage, productivity_score) ', @sql, ' ON DUPLICATE KEY UPDATE estimated_contribution = VALUES(estimated_contribution), expected_contribution = VALUES(expected_contribution), author_coding_time_percentage = VALUES(author_coding_time_percentage), productivity_score = VALUES(productivity_score)');
       
    PREPARE stmt FROM @insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$

DELIMITER ;


-- Benchmark New Feature vs Maint
-- select CONCAT('select sum(period_investment) period_investment, sum(growth_investment)  growth_investment from `', t.TABLE_SCHEMA ,'`.cooked_data cd where aggregate_type = 0 union all')  
-- from information_schema.TABLES t where t.TABLE_SCHEMA <> 'master' and t.TABLE_NAME = 'cooked_data'

-- select round(sum(growth_investment)*100/sum(period_investment),0) "new_features",100-round(sum(growth_investment)*100/sum(period_investment),0) "maintenance" from
-- (
	-- *** resultado query generadora***
-- ) q

-- select round(sum(growth_investment)*100/sum(period_investment),0) "new_features",100-round(sum(growth_investment)*100/sum(period_investment),0) "maintenance" from TENANT.cooked_data cd where aggregate_type = 0

DELIMITER $$

CREATE PROCEDURE benchmark_new_features_maintenance(IN period_param BIGINT DEFAULT NULL)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE tenant_name VARCHAR(256);
    DECLARE new_features_val DOUBLE;
    DECLARE maintenance_val DOUBLE;
    DECLARE default_period BIGINT;
    DECLARE cur CURSOR FOR SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN (SELECT tenant FROM ignore_tenant);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Calculate the default period if not provided
    IF period_param IS NULL THEN
        SET default_period = EXTRACT(YEAR FROM DATE_SUB(NOW(), INTERVAL 1 MONTH)) * 100 +
                                 EXTRACT(MONTH FROM DATE_SUB(NOW(), INTERVAL 1 MONTH));
    ELSE
        SET default_period = period_param;
    END IF;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO tenant_name;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Calculate new_features and maintenance for each tenant
        SET @sql = CONCAT('SELECT ROUND(SUM(growth_investment) * 100 / SUM(period_investment), 0) AS new_features, 100 - ROUND(SUM(growth_investment) * 100 / SUM(period_investment), 0) AS maintenance FROM ', tenant_name, '.cooked_data WHERE aggregate_type = 0 AND version_from = ', default_period);
        
        PREPARE stmt FROM @sql;
        EXECUTE stmt INTO new_features_val, maintenance_val;
        DEALLOCATE PREPARE stmt;

        -- Insert or update the new_features_maintenance table
        INSERT INTO new_features_maintenance (period, tenant, new_features, maintenance) VALUES (default_period, tenant_name, new_features_val, maintenance_val)
        ON DUPLICATE KEY UPDATE new_features = VALUES(new_features), maintenance = VALUES(maintenance);
    END LOOP;

    CLOSE cur;
END$$

DELIMITER ;


-- #################################################### BENCKMARK DATABASE ####################################################

CREATE DATABASE benchmark;

CREATE TABLE `ignore_tenant` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `tenant` varchar(256) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `productivity` (
  `period` bigint NOT NULL,
  `estimated_contribution` double NULL,
  `expected_contribution` double NULL,
  `author_coding_time_percentage` double NULL,
  `productivity_score` double NULL,
  PRIMARY KEY (`period`)
);

CREATE TABLE `new_features_maintenance` (
  `period` bigint NOT NULL,
  `tenant` varchar(256) NOT NULL,
  `new_features` double NULL,
  `maintenance` double NULL,
  PRIMARY KEY (`period`, `tenant`)
);
