from sqlalchemy import create_engine,select,MetaData,Table,insert,delete
from sqlalchemy.exc import SQLAlchemyError,DBAPIError
import logging
import threading 
import pymysql
import psycopg2
metadata=MetaData()





#源数据库jbdc
engine=create_engine('mysql+pymysql://root:13774529926@localhost:3306/py',pool_recycle=3600,pool_pre_ping=True,max_overflow=100, pool_size=100)

#目标数据库jbdc
engine2=create_engine('postgresql+psycopg2://postgres:13774529926@localhost:5432/postgres',pool_recycle=3600,pool_pre_ping=True,max_overflow=100, pool_size=100)




#数据表映射,源数据库表k：目标数据库表v
My_Table={
          'tt':'tt_copy2',
          'test':'test_post',
          'test':'test_post_copy1'
}


def my_init():
    #连接池初始化
    try:
        for i in range(len(My_Table)+2):
            c1=engine.connect()
            c2=engine2.connect()
            c1.execute('select 1')
            c2.execute('select 1')
            c1.close()
            c2.close()
    except DBAPIError as dberror:
        logging.error(str(dberror))
    #日志设置
    logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG,
                    filename=r'C:\Users\aaa\Desktop\test.log',
                    filemode='a')
    logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)

def my_migration(k,v):

    try:
        con1=engine.connect()
        con2=engine2.connect()  
    except DBAPIError as dberror:
        logging.error(str(dberror))
    map1=Table(k,metadata,autoload=True,autoload_with=engine)
    map2=Table(v,metadata,autoload=True,autoload_with=engine2)
    map2_delete=delete(map2) #删除目标数据表数据
    logging.info('删除目标数据库表'+v+str(con2.execute(select(map2)).rowcount)+'条数据！')
    con2.execute(map2_delete)
    #map1_key=map1.columns.keys()
    #map2_key=map2.columns.keys()
    map1_select=select(map1)
    results=con1.execute(map1_select).fetchall()
    data = [dict(zip(result.keys(), result)) for result in results]
    #批量插入数据
    try:
        ins=insert(map2)
        con2.execute(ins,data)
        logging.info('源数据库表'+k+'到目标数据库表'+v+'更新数据'+str(len(results))+'条！')
    except SQLAlchemyError as error:
        logging.error(str(error))

    con1.close()
    con2.close()





if __name__ == '__main__':


    my_init()

    thread_pool = [] #开启多线程
    for key,value in My_Table.items():
        thread_pool.append(threading.Thread(target=my_migration,args=(key,value)))
        #my_migration(key,value)
    for th in thread_pool:
        th.start()

    for th in thread_pool:
        th.join()

    logging.info('执行完毕！')

    
    


    





