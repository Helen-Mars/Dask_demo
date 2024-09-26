# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 14:05:10 2024

@author: mhlong
"""

from dask.distributed import Client, wait
import multiprocessing
import subprocess as sp
import logging
import json
import os


# with open(r'C:\Users\SDJ-JSJ074\Desktop\任务级并行工具\project_setting.json', encoding='utf-8-sig') as json_file:
#     project_setting = json.load(json_file, object_pairs_hook=OrderedDict)

program_data_path = os.environ.get('PROGRAMDATA')
setting_path = os.path.join(program_data_path, 'EastWave', 'project_setting_parallel_multidask.json')

with open(setting_path, encoding='utf-8') as f:
    project_setting = json.load(f)

# current_path = project_setting['work_file']['path']
# os.chdir(current_path)

tasks_set = project_setting['tasks_set']



# 构造接口数据
# mxi_path = "E:/ew_project/ew_bin/ew7/x64/mxi.exe"
# work_catalog = "C:/Users/SDJ-JSJ074/Desktop/任务级并行工具/"
# task = ["h=600;r=70", "h=800;r=90"]
# df_column_name = ['h', 'r']


thread = project_setting['thread']
n_workers = eval(project_setting['n_workers'])
solver_eastwave = project_setting['solver_eastwave_path']
east_solver_config = project_setting['eastwave_solver_config']


file_path = tasks_set[0][0]
direct, filename = os.path.split(file_path)

# 记录运算结果
futures = []
solver_result = []


def process_file(task):
    
    east_config = [solver_eastwave, \
                        "-np", thread,\
                          task]
        
    for key, value in east_solver_config.items():
        if value != "":
            east_config.append(value)    
        
        
    solver_flag = sp.run(east_config) 
        
    if solver_flag.returncode != 0:
        raise Exception("Eastwave solver fialed to process ewp2_file!")
           

    return solver_flag.returncode


def main(): 
# 配置log日志文件
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s -%(levelname)s - %(message)s')
    file_handle = logging.FileHandler(direct+"/" + 'log.txt')
    file_handle.setLevel(logging.INFO)
    file_handle.setFormatter(logging.Formatter('%(asctime)s - %(name)s -%(levelname)s - %(message)s'))
    logger = logging.getLogger("solver_log")


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
    for x in tasks_set:
        future = client.submit(process_file, x[0])
        
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


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
    os.system("pause")
