from pymongo import MongoClient
import pandas as pd
from get_sql_miller import Connector22, Connector158, ConnectorJavaTest, ConnectorPublish
import requests
import json
import concurrent.futures
import time
from datainser import insert as it
import logging

# 设置日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Queue():
    def __init__(self, sourceid='', site='', read_sku_sql='', limit=999999, priority=1):
        self.sourceid = sourceid
        self.read_sku_sql = read_sku_sql
        self.site = site
        self.limit = limit
        self.priority = priority

    def read_new_data(self):
        try:
            logging.info("开始读取新数据")

            # 禁运数据补充
            temp = ConnectorJavaTest().read_sql(f'''select * from t_mercadolibre_logistics_providers WHERE ordersource_id = {self.sourceid} and site_id = "{self.site}"''')
            name = temp.loc[0, ['Logistics_provider_name']].values[0]
            logging.info(f"禁运数据读取完成，物流提供商名称：{name}")

            # 基础数据整理,并检测kyc状态
            skudata = ConnectorJavaTest().read_sql(self.read_sku_sql)
            logging.info("SKU数据读取完成，正在进行基础数据整理")

            sysdata = ConnectorJavaTest().read_sql('''select * from t_mercadolibre_158_basic''')
            pricedata = ConnectorJavaTest().read_sql('''select * from t_mercadolibre_price''')
            pricedata = pricedata.loc[:, ['sku', 'site', 'profit_30']]
            pricedata = pricedata.rename(columns={'profit_30': 'price'})
            lognegdata = ConnectorJavaTest().read_sql(f'''select WithBatteryValue as 'attributes',{name} as "neg" from t_mercadolibre_logistics_negativelist''')
            lognegdata['neg'] = lognegdata['neg'].astype(int)
            catenegdata = ConnectorJavaTest().read_sql(f'''select category_id,{self.site} as "restrict" from t_mercadolibre_site_restricted''')
            catenegdata['restrict'] = catenegdata['restrict'].astype(int)
            sourcedata = ConnectorJavaTest().read_sql(f'''select * from t_mercadolibre_status where source_id={self.sourceid} and site_id = "{self.site}" ''')
            logging.info("基础数据整理完成")

            if sourcedata.empty:
                logging.warning('无站点可以录入,可能是因为录入店铺均被kyc')
                return

            source_name = sourcedata.loc[0, ['source_name']].values[0]

            # 匹配数据,不刊登无库存,仿牌等数据
            data = pd.merge(skudata, pricedata, how='inner', on=['sku', 'site'], suffixes=('_skudata', '_pricedata'))
            data = pd.merge(data, sysdata, how='left', on=['sku'], suffixes=('_data', '_sysdata'))
            data = data[(data['inventory'] >= 1) & (data['if_fake'] == 0) & (data['if_warrently'] == 0)]
            logging.info(f"数据匹配完成，有效数据条数：{len(data)}")

            # 匹配禁运禁售数据,禁售属性数据
            data = pd.merge(data, lognegdata, how='left', on=['attributes'])
            data = data[data['neg'] != 1]
            data = pd.merge(data, catenegdata, how='left', on=['category_id'])
            data = data[data['restrict'] != 1]
            logging.info(f"禁运禁售状态匹配完成，有效数据条数：{len(data)}")

            # title-命名标题如何命名，priority-优先级如何设置，key_words-关键词埋词如何设置，publish_status-资料状态需要修改
            data['source_id'] = self.sourceid
            data['source_name'] = source_name
            data['site_id'] = self.site
            data['title'] = None
            data['priority'] = self.priority
            data['key_words'] = None
            data['publish_status'] = 1

            data['source_sku'] = None
            data['global_item_id'] = None
            data['site_item_id'] = None
            data['upc'] = None
            data['currency'] = 'USD'
            data['upc'] = None
            data['attributes'] = None
            data['pictures'] = None
            data['update_attribute'] = 0

            data['error_status'] = None
            data['err_msg'] = None
            data['retry_num'] = 3
            data['customer_id'] = 1
            data['create_by'] = 63008
            data['publish_time'] = None
            data['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            data['update_by'] = 63008

            data = data.rename(columns={'ordersourceid': 'source_id', 'ordersourcename': 'source_name',
                                        'inventory': 'quantity'})
            data = data.loc[:, ['source_id', 'source_name', 'site_id', 'title', 'sku',
                                'source_sku', 'global_item_id', 'site_item_id', 'upc', 'price',
                                'quantity', 'currency', 'category_id', 'category_name', 'pictures',
                                'update_attribute', 'priority', 'key_words', 'publish_status',
                                'error_status', 'retry_num', 'err_msg', 'remark', 'publish_time',
                                'customer_id', 'create_by', 'create_time', 'update_by']]
            data['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            data['is_deleted'] = 0
            logging.info("数据处理完毕，准备入库")

            it(data, 't_mercadolibre_product_publish_execute',
               updateset=['title', 'source_sku', 'global_item_id', 'site_item_id', 'upc', 'price', 'quantity',
                          'currency', 'category_id', 'category_name', 'pictures', 'update_attribute', 'priority',
                          'key_words', 'publish_status', 'error_status', 'retry_num', 'err_msg', 'remark', 'publish_time',
                          'customer_id', 'create_by', 'create_time', 'update_by', 'update_time', 'is_deleted'],
               mergekey=['source_id', 'site_id', 'sku'], uniquekey='id')
            logging.info("数据入库完成")

        except Exception as e:
            logging.error(f"在处理数据时发生错误: {e}")

if __name__ == '__main__':
    while True:
        try:
            logging.info("主程序开始执行")

            # 处理MLC站点

            sourceids_MLC = [10830, 11912, 15106, 15116, 15139,16071,17974,18088]
            for sourceid_MLC in sourceids_MLC:
                SQLMLC =f'''select distinct a.* from t_mercadolibre_sift as a
                    left join t_mercadolibre_price as b on b.sku = a.sku and a.site = b.site
                    where a.is_deleted != 1 and a.category_id != '1' and a.site = 'MLC' and b.profit_30 is not null
                    AND a.category_id in ('CBT116632', 'CBT29933', 'CBT399140', 'CBT126137', 'CBT37750', 'CBT388313', 'CBT18398', 'CBT9318', 'CBT30091', 'CBT357155', 'CBT432838', 'CBT95200', 'CBT418448', 'CBT429735', 'CBT1386', 'CBT388595', 'CBT412470', 'CBT414172', 'CBT417376', 'CBT377242', 'CBT411071', 'CBT411421', 'CBT412737', 'CBT417271', 'CBT392452', 'CBT431802', 'CBT65848', 'CBT412370', 'CBT417218', 'CBT371458', 'CBT392132', 'CBT412499', 'CBT413410', 'CBT417267', 'CBT424491', 'CBT95333')
                    ORDER BY a.id asc;'''
                logging.info(f"正在处理Source ID: {sourceid_MLC}")
                df = ConnectorJavaTest().read_sql(SQLMLC)
                logging.info(f"数据读取完成，行数: {len(df)}")
                
                MLCa = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                queue = Queue(sourceid=sourceid_MLC, site='MLC', read_sku_sql=SQLMLC, limit=40000, priority=3)
                queue.read_new_data()
                MLCb = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                
                logging.info(f"Source ID: {sourceid_MLC} 处理完成")
                logging.info(f"开始时间: {MLCa}")
                logging.info(f"结束时间: {MLCb}")

            # 处理MCO站点
            sourceids_MCO = []
            # sku = ('11922352')
            for sourceid_MCO in sourceids_MCO:
                SQLMCO =f'''select distinct a.* from t_mercadolibre_sift as a
                    left join t_mercadolibre_price as b on b.sku = a.sku and a.site = b.site
                    where a.is_deleted != 1 and a.category_id != '1' and a.site = 'MCO' and b.profit_30 is not null
                    AND a.category_id in ('CBT39948', 'CBT411151', 'CBT105404', 'CBT418042', 'CBT416719', 'CBT6098', 'CBT412056', 'CBT412101', 'CBT11036', 'CBT74488', 'CBT8321', 'CBT429637', 'CBT416721', 'CBT416811', 'CBT416961', 'CBT30840', 'CBT402997', 'CBT90303', 'CBT3518', 'CBT417981', 'CBT455888', 'CBT370542', 'CBT413591', 'CBT417718', 'CBT90319', 'CBT400952', 'CBT399134', 'CBT455859', 'CBT406032', 'CBT414038', 'CBT2662', 'CBT39976', 'CBT439151', 'CBT95410', 'CBT401563', 'CBT416675', 'CBT418305', 'CBT413422', 'CBT48900', 'CBT91758', 'CBT9973', 'CBT10096', 'CBT11033', 'CBT2968', 'CBT1667', 'CBT417226', 'CBT1287', 'CBT4606', 'CBT74609', 'CBT90315', 'CBT117259', 'CBT455515', 'CBT63262', 'CBT388855', 'CBT455893', 'CBT90322', 'CBT392531', 'CBT417006', 'CBT430974', 'CBT100778', 'CBT414192', 'CBT456660', 'CBT378162', 'CBT69938', 'CBT11040', 'CBT29316', 'CBT375288', 'CBT29309', 'CBT2997', 'CBT352343', 'CBT412506', 'CBT417395', 'CBT417891', 'CBT61216', 'CBT109027', 'CBT4282', 'CBT73676', 'CBT86375', 'CBT431678', 'CBT416691', 'CBT62937', 'CBT7115', 'CBT81636', 'CBT86844', 'CBT412515', 'CBT413440', 'CBT414247', 'CBT435148', 'CBT105405', 'CBT29901', 'CBT411910', 'CBT416559', 'CBT414223', 'CBT417898', 'CBT94881', 'CBT126136', 'CBT40385', 'CBT430308', 'CBT43676', 'CBT7524', 'CBT387584', 'CBT429417', 'CBT414212', 'CBT417127', 'CBT433023', 'CBT109089', 'CBT3568', 'CBT352679', 'CBT428934', 'CBT411766', 'CBT6049', 'CBT65824', 'CBT417099', 'CBT455865', 'CBT398575', 'CBT31439', 'CBT413460', 'CBT90306', 'CBT29890', 'CBT414091', 'CBT428980', 'CBT372115', 'CBT411483', 'CBT416774', 'CBT73917', 'CBT429711', 'CBT371434', 'CBT411912', 'CBT413345', 'CBT416239', 'CBT429015', 'CBT432055', 'CBT455760', 'CBT9766', 'CBT1345', 'CBT389307', 'CBT47475', 'CBT36551', 'CBT3738', 'CBT417183', 'CBT109282', 'CBT433769', 'CBT3559', 'CBT24682', 'CBT29907', 'CBT409460', 'CBT4619', 'CBT6114', 'CBT95955', 'CBT40513', 'CBT400982', 'CBT4702', 'CBT109104', 'CBT400173', 'CBT409254', 'CBT411152', 'CBT352337', 'CBT380656', 'CBT416552', 'CBT436797', 'CBT429740', 'CBT53260', 'CBT24788', 'CBT401156', 'CBT412513', 'CBT417559', 'CBT432665', 'CBT7854', 'CBT371991', 'CBT416545', 'CBT416946', 'CBT431046', 'CBT8937', 'CBT90321', 'CBT388865', 'CBT393761', 'CBT392701', 'CBT39964', 'CBT1714', 'CBT361678', 'CBT371429', 'CBT37616', 'CBT413465', 'CBT74611', 'CBT370528', 'CBT389336', 'CBT392393', 'CBT429117', 'CBT430511', 'CBT7141', 'CBT37615', 'CBT414088', 'CBT414164', 'CBT432000', 'CBT1161', 'CBT29902', 'CBT388818', 'CBT388863', 'CBT409360', 'CBT412637', 'CBT413558', 'CBT416810', 'CBT436433', 'CBT72278', 'CBT373509', 'CBT414190', 'CBT416823', 'CBT50089', 'CBT7661', 'CBT109100', 'CBT30759', 'CBT412077', 'CBT413474', 'CBT7408', 'CBT126135')
                    ORDER BY a.id asc;'''
                logging.info(f"正在处理Source ID: {sourceid_MCO}")
                df = ConnectorJavaTest().read_sql(SQLMCO)
                logging.info(f"数据读取完成，行数: {len(df)}")
                

                MCOa = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                queue = Queue(sourceid=sourceid_MCO, site='MCO', read_sku_sql=SQLMCO, limit=40000, priority=3)
                queue.read_new_data()
                MCOb = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                
                logging.info(f"Source ID: {sourceid_MCO} 处理完成")
                logging.info(f"开始时间: {MCOa}")
                logging.info(f"结束时间: {MCOb}")

            logging.info("所有数据处理完成")
            break

        except Exception as e:
            logging.error(f"主程序运行时发生错误: {e}")
            continue
