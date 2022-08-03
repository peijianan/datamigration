from sqlalchemy import create_engine,select,MetaData,Table,insert,delete
import pymysql
import psycopg2
from sqlalchemy.exc import SQLAlchemyError
import logging


#日志设置
logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG,
                    filename=r'C:\Users\aaa\Desktop\test.log',
                    filemode='a')
logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)


#源数据库jbdc
engine=create_engine('mysql+pymysql://root:13774529926@localhost:3306/py',pool_recycle=3600,pool_pre_ping=True,max_overflow=100, pool_size=100)

#目标数据库jbdc
engine2=create_engine('postgresql+psycopg2://postgres:13774529926@localhost:5432/postgres',pool_recycle=3600,pool_pre_ping=True,max_overflow=100, pool_size=100)



#数据表映射,源数据库表k：目标数据库表v
My_Table={
          'tt':'tt_copy2',
          'test':'test_post'
}


def my_migration(k,v):
    metadata=MetaData()
    map1=Table(k,metadata,autoload=True,autoload_with=engine)
    map2=Table(v,metadata,autoload=True,autoload_with=engine2)
    map2_delete=delete(map2)
    #map1_key=map1.columns.keys()
    #map2_key=map2.columns.keys()
    engine2.execute(map2_delete)
    map1_select=select(map1)
    results=engine.execute(map1_select).fetchall()
    data = [dict(zip(result.keys(), result)) for result in results]
    
    try:
        ins=insert(map2)
        engine2.execute(ins,data)
        logging.info('源数据库表'+k+'到目标数据库表'+v+'更新数据'+str(len(results))+'条！')
    except SQLAlchemyError as error:
        logging.error(error.orig.message, error.params)





if __name__ == '__main__':
    for key,value in My_Table.items():
        my_migration(key,value)
    logging.info('执行完毕！')

    
    


    





