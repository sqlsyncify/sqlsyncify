DELIMITER $$

DROP FUNCTION IF EXISTS `getCategoryParentListObj`$$

CREATE FUNCTION  `getCategoryParentListObj`(rootIds VARCHAR(4000)) RETURNS varchar(4000) CHARSET utf8
    DETERMINISTIC
BEGIN
    DECLARE sParentList VARCHAR(4000);
    DECLARE sParentTemp VARCHAR(4000);
    DECLARE returnTemp VARCHAR(4000);
    DECLARE prevParentList VARCHAR(4000);
    DECLARE loopCounter INT DEFAULT 0;
    DECLARE maxLoops INT DEFAULT 10;

    -- 初始化变量
    SET sParentTemp = rootIds;
    SET sParentList = '';
    SET prevParentList = '';

    -- 防止无限循环
    WHILE sParentTemp IS NOT NULL AND sParentTemp != '' AND loopCounter < maxLoops DO
        SET loopCounter = loopCounter + 1;
        
        -- 保存当前状态用于比较
        SET prevParentList = sParentList;
        
        -- 查询当前层级的父分类信息
    SELECT
    GROUP_CONCAT(DISTINCT wtt.parent),
    -- mysql version < 5.7
    /*
    GROUP_CONCAT(DISTINCT CONCAT(
                '{"catId":', wt.term_id,
                ',"catName":"', REPLACE(REPLACE(wt.name, '"', '\\"'), '\\', '\\\\'),
                '","parent":', wtt.parent, '}'
            ))
            */
    -- mysql version >= 5.7
    GROUP_CONCAT(
                JSON_OBJECT(
                'catId', wt.term_id,
                'catName', wt.name,
                'parent', wtt.parent
                )
            )
        INTO
            sParentTemp,
            returnTemp
        FROM wp_term_taxonomy AS wtt
                 LEFT JOIN wp_terms wt ON wtt.term_id = wt.term_id
        WHERE wtt.taxonomy = 'product_cat'
      AND FIND_IN_SET(wt.term_id, sParentTemp) > 0
      ;

        -- 处理NULL值
        SET sParentTemp = IFNULL(sParentTemp, '');
        SET returnTemp = IFNULL(returnTemp, '');
        
        -- 将结果添加到列表中
        IF returnTemp != '' THEN
            IF sParentList != '' THEN
                SET sParentList = CONCAT(sParentList, ',', returnTemp);
            ELSE
                SET sParentList = returnTemp;
            END IF;
        END IF;
        
        -- 如果没有更多父级或者结果没有变化，则退出循环
        IF sParentTemp = '' OR sParentList = prevParentList THEN
            SET sParentTemp = NULL;
        END IF;

    END WHILE;

RETURN sParentList;
END$$

DELIMITER ;