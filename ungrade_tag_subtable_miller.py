from get_sql_miller import ConnectorJavaTest
import pandas as pd
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# 初始化数据库连接
db_connector = ConnectorJavaTest()

# 定义 holiday_id 和 tagids 的对应关系
# 例如：{43: [8, 40], 44: [10, 20]}
holiday_tag_mapping = {
    2: [7,27,50,82],
    3: [11,30],
    4: [15,42,76,79],
    5: [20,46,59,71],
    7: [16],
    8: [80,81],
    9: [1,22,49,60],
    11: [4,33],
    41: [6],
    42: [12,35],
    43: [8,40]
    # 在这里添加更多的对应关系
}

try:
    for holiday_id, tagids in holiday_tag_mapping.items():
        logger.info(f"正在从数据库读取 holiday_id {holiday_id} 的数据...")

        read_query = f'''
        SELECT a.sku
        FROM t_mercadolibre_sift a
        LEFT JOIN rb_product b ON b.sku = a.sku
        WHERE a.is_deleted = 0 and b.holiday_id = {holiday_id};
        '''
        data = db_connector.read_sql(read_query)

        logger.info(f"数据读取完成，holiday_id {holiday_id} 共找到 {len(data)} 条记录。")

        logger.info("开始处理数据...")

        for sku in data['sku']:
            logger.info(f"处理 SKU: {sku}")

            for tagid in tagids:
                logger.info(f"检查 SKU {sku} 对于 tagid {tagid} 的记录...")
                check_query = f'''
                SELECT COUNT(*) AS count
                FROM t_mercadolibre_tag_subtable
                WHERE sku = '{sku}' AND tagid = {tagid};
                '''
                check_result = db_connector.read_sql(check_query)

                if check_result.iloc[0]['count'] == 0:
                    logger.info("未找到记录，插入新记录...")
                    insert_query = f'''
                    INSERT INTO t_mercadolibre_tag_subtable (sku, tagid)
                    VALUES ('{sku}', {tagid});
                    '''
                    db_connector.execute(insert_query)
                else:
                    logger.info("已存在记录，跳过插入。")

    logger.info('所有操作完成')

except Exception as e:
    logger.error(f"发生错误: {e}")
