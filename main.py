# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 14:05:10 2024

@author: mhlong
"""

from dask.distributed import Client, wait
import multiprocessing
import subprocess as sp
import logging
import pandas as pd
import json
import os

# with open(r'C:\Users\SDJ-JSJ074\Desktop\任务级并行工具\project_setting.json', encoding='utf-8-sig') as json_file:
#     project_setting = json.load(json_file, object_pairs_hook=OrderedDict)

program_data_path = os.environ.get('PROGRAMDATA')
setting_path = os.path.join(program_data_path, 'EastWave', 'project_setting_parallel_multidask.json')

with open(setting_path, encoding='utf-8') as f:
    project_setting = json.load(f)

current_path = project_setting['work_file']['path']
os.chdir(current_path)

var = [[i[0], eval(i[1])] for i in project_setting['var_table']]

def pick_var(x):
    if len(x) == 0:
        return['']
    else:
        rest = pick_var(x[1:])                
        new = []
        for num in x[0][1]:
            for y in rest:
                new.append(x[0][0]+'='+str(num)+';'+ y)
            
    return new
    
total_task = pick_var(var)
prepare_total_task = [string.rstrip(';') for string in total_task]      
prepare_df_column_name = [item.split("=")[0] for item in prepare_total_task[0].split(';')]


# 构造接口数据
# mxi_path = "E:/ew_project/ew_bin/ew7/x64/mxi.exe"
# work_catalog = "C:/Users/SDJ-JSJ074/Desktop/任务级并行工具/"
# task = ["h=600;r=70", "h=800;r=90"]
# df_column_name = ['h', 'r']
task = prepare_total_task
df_column_name = prepare_df_column_name
work_catalog = project_setting['work_file']['path']+"/"
mxi_path = project_setting['mxi_path']
thread = project_setting['thread']
n_workers = eval(project_setting['n_workers'])
solver_eastwave = project_setting['solver_eastwave_path']
work_file = project_setting['work_file']
mxd_file = project_setting['deal_mxd_path']
east_solver_config = project_setting['eastwave_solver_config']


# 配置log信息
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s -%(levelname)s - %(message)s')
file_handle = logging.FileHandler(work_catalog + 'log.txt')
file_handle.setLevel(logging.INFO)
file_handle.setFormatter(logging.Formatter('%(asctime)s - %(name)s -%(levelname)s - %(message)s'))
logger = logging.getLogger("solver_log")
# 记录运算结果
futures = []
solver_result = []


def process_file(task, count):
    
    aim_catalog = work_catalog+str(count)+"/"
    
    east_config = [solver_eastwave, \
                          "-np", thread,\
                          "-D_FILE", aim_catalog,\
                          "-set-var", task,\
                          work_file['fullpath']]
        
    for key, value in east_solver_config.items():
        if value != "":
            east_config.append(value)
    
    
    solver_flag = sp.run(east_config) 
        
    if solver_flag.returncode != 0:
        raise Exception("Eastwave solver fialed to process ewp2_file!")
        
    with open(aim_catalog+'param.txt', 'w') as f:
        f.write("param: \n"+task)     
    

    return solver_flag.returncode


def main(): 

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)    
    logger.addHandler(file_handle)
    
    if n_workers == None:
        client  = Client()  
    else:
        client  = Client(n_workers=n_workers)          

    
    scheduler_info = client.scheduler_info()
    dashboard_url = client.dashboard_link
    
    logger.info("Dashboard Url: %s", dashboard_url)
    logger.info("Scheduler address: %s", scheduler_info['address'])
    logger.info("Scheduler services: %s", scheduler_info['services'])
    logger.info("Scheduler workers: %s", scheduler_info['workers'])
    
    count = 0
    for x in task:
        future = client.submit(process_file, x, count)
        
        futures.append(future) 
        count += 1
    
    future_status = wait(futures)
    logger.info(future_status)  
    
    # 需要记录solver执行的结果
    for future in futures:
         solver_result.append(future.result())
         
    logger.info("solver_return: %s", solver_result)
    
    has_nonzero = any(num!=0 for num in solver_result)
    
    if(has_nonzero == True):
        raise Exception("Some solvers did not terminate successfully!")
    
    logger.info("Finished solver tasks and client will exit!")
    
    client.close() 

    #进入后处理
    try:
        post_tasks = []  
        for i in range(len(task)):
            param = None
            with open(work_catalog+str(i)+"/param.txt", 'r') as f:
                lines = f.readlines()
                param = lines[1].strip()
            temp = [param, work_catalog+str(i)+"/", work_catalog + str(i)+"/"+ ".data/"]
            post_tasks.append(temp)     
       
        with open(work_catalog+'mxi_setting.json', 'w', encoding='utf-8') as f:
            json.dump(post_tasks, f, ensure_ascii=False)
        
    
        return_code = sp.run([mxi_path, mxd_file], shell=True, encoding="utf-8")
        if return_code.returncode != 0:
            raise Exception("The mxi did not finish processing mxd files properly!" )
            
            
        df = pd.read_csv(work_catalog+"mxi_deal.txt", delimiter=' ', skiprows=0, header=1)
        
        df = df.iloc[:, :-1]
        
        df[df_column_name] = df['param'].str.split(';', expand=True)
        
        df= pd.concat([df[df_column_name], df.drop(columns=df_column_name)], axis=1)
        
         
        for i in df_column_name:
            
            condition = df[i].str.startswith(i+'=')
            
            df.loc[condition, i] = df.loc[condition, i].str.replace(i+'=', '').astype(float)
            
        df.to_csv(work_catalog+'results.csv', index=False)
        
        logger.info("Finished post-peocessing tasks!")
        
    except Exception as e:
        logger.info("post_processing error: %s", e)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
    os.system("pause")
