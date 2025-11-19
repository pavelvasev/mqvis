#!/bin/env python3.9

"""
Программа отображения очереди задач Slurm

Замысел: показать узлы и на них назначенные и запланированные задачи,
чтобы можно было видеть когда что реально запустить, и что недозагружено.

Запуск:
* python3.9 mqvis.py
напечатает таблицу псевдокодом

* FORMAT=html python3.9 mqvis.py
напечатает веб-страницу

фичи:
- подсветка некоторых узлов (apollo17-36, tesla-hi)
- разбивка по 8 часов для удобства восприятия
- отображение в несколько колонок чтобы все влезало на экран
- показать недозагруженные узлы (по процессорам)
- выдача в текстовом и хтмл форматах (режим text, режим html)
- управление параметрами через env 
- подсветка недозанятых узлов (0<freecpu<total) #F-NODE-NONBUSY-HILITE
- подсветка границ суток #F-HILITE-DAY
- показывать колонки не от текущего времени а по времени суток, чтобы понимать 
вот 6 утра, вот 12, вот 18... и оно хорошо сойдется с границей суток #F-CURHOUR-SHIFT

режим text:
- подстветка задач выбранного (текущего) пользователя #F-HILITE-USER-TASKS
- отображение номеров задач выбранного (текущего) пользователя #F-SHOW-USER-TASKS
  причем с разбивкой на 2 группы - работающие и pending

режим html:
- вывод списка пользователей #F-USERS
- умение скрывать пользователей (замена на задачи) #F-HIDE-USERS
- tooltip на ячейках с информацией о времени, пользователях и задаче #F-TOOLTIP
- возможность кликнуть по имени пользователя и подсветить все его задачи #F-CLICK-USER
- фиксация выбранного пользователя в параметрах урля
- автоматическое определение колонок по ширине экрана #F-AUTO-COLS
- вывод текущего времени вверху страницы, для контроля актуальности #F-CURTIME
- автоперезагрузка страницы
- подсветка недозанятых узлов с градацией цвета по недозанятости #F-NONBUSY-PARTIAL
- показывать число работающих задач во временнОм слоте #F-JOB-CNT
- подписать имя ехе-шника в тултипе, как это сделано в таблице узлов; в режиме #F-HIDE-USERS так сделано

идеи:
- подписать вверху и внизу на каждом блоке время его начала
- либо - подсветить границу суток
- подсветить выбранную задачу (по клику на ячейку? по клику на пользователя показать список его задач и по ним выбирать?)
- показывать инфу и по видеокартам в столбце загрузки узла
  и мб недозагруженным
  и также нужно по памяти свободной
  можно сделать в html переключатель что показывать - cpu, gpu, mem
- разбить узлы на группы (аполло слабые, аполло сильные, хайперф, отладочные..)
- впечатать шаблон в питон, чтобы был 1 файл скрипта и все
- выдача с ключем --details по задачам включает информацию по процессорам на узлах, это можно использовать для анализа нагрузки во времени
- показать нагрузку на узлы более детально, см squeue -d


2025 (c) Павел Васёв, сектор компьютерной визуализации
отдела системного обеспечения ИММ УрО РАН, г. Екатеринбург
www.cv.imm.uran.ru vasev@imm.uran.ru
"""

import os

################ параметры
# для текста и для html
# в каком формате выдать: html или text
FORMAT = os.environ.get("FORMAT","text")
# кол-во ячеек часов
# Валидация TIME_SLOTS для предотвращения DoS
try:
    TIME_SLOTS = int(os.environ.get("SLOTS","40"))
    if TIME_SLOTS < 1 or TIME_SLOTS > 500:
        print(f"Warning: SLOTS value {TIME_SLOTS} is out of range [1-500], using default 40", file=sys.stderr)
        TIME_SLOTS = 40
except ValueError:
    print(f"Warning: Invalid SLOTS value, using default 40", file=sys.stderr)
    TIME_SLOTS = 40

# для текстовой версии
# подсветить пользователя #F-HILITE-USER-TASKS
HILITE_USER = os.environ.get("USER","-") # 'u1321'
# сколько колонок выдать
COLUMNS = 2
# по сколько часов разбивать
SLOT_ITEMS = 6
###############
#F-HIDE-USERS
HIDE_USERS = False if "FORMAT" == "text" else True
# детальная информация о нагрузке по часам (мб. долго)
# todo это глючит - не сходятся числа cpu надо разбираться
DETAILED_USAGE = False
# показывать число задач #F-JOB-CNT
SHOW_JOB_CNT = True


import sys
import subprocess
import json
from pathlib import Path
import traceback
import csv
import io
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
import re
import html


def simple_sinfo_dict():
    """
    Простая версия для получения списка узлов SLURM
    """
    try:
        # Выполняем команду
        # добавлено -a чтобы работало под апачем
        cmd = subprocess.run(['sinfo', '-N', '-a', '--noheader', '-o', '%N %C %t %P'], 
                           capture_output=True, text=True, check=True)
        
        nodes = {}
        
        for line in cmd.stdout.strip().split('\n'):
            if line.strip():
                parts = line.split()
                if len(parts) >= 4:
                    node_name = parts[0]
                    
                    cpu_numbers = [int(x) for x in parts[1].split('/')] # allocated / idle / other / total
                    
                    if node_name in nodes:
                      nodes[node_name]['partitions'].append( parts[3] )
                    else:                    
                      nodes[node_name] = {
                          'cpus': parts[1],
                          'cpus_free': cpu_numbers[1],
                          'cpus_total': cpu_numbers[3],
                          'state': parts[2], 
                          'partitions': [parts[3]]
                      }
        
        return nodes
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return {}


def get_jobs_dataframe():
    """
    Выполняет команду squeue -o "%all" --states=PENDING и возвращает DataFrame
    """
    try:
        # Выполняем команду squeue
        # добавлено -a чтобы работало под апачем
        result = subprocess.run(
            ['squeue', '-a', '-o', '%all'],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Проверяем, есть ли данные
        if not result.stdout.strip():
            print("Нет задач")
            return []
        
        # squeue выводит данные в табличном формате с разделителем |
        f = io.StringIO(result.stdout)
        reader = csv.DictReader(f, delimiter='|', skipinitialspace=True)

        cleaned = []
        for row in reader:
            new_row = {}
            for k, v in row.items():
                if k is None:
                    continue
                key = k.strip()
                if key == '':
                    # пропускаем пустые имена колонок (если такие есть)
                    continue
                new_row[key] = v.strip() if isinstance(v, str) else v
            cleaned.append(new_row)

        return cleaned        

        
    except subprocess.CalledProcessError as e:
        print(f"Ошибка выполнения команды squeue: {e}")
        print(f"Stderr: {e.stderr}")
        return []
    except FileNotFoundError:
        print("Команда squeue не найдена. Убедитесь, что SLURM установлен.")
        return []
    except Exception as e:
        print(f"Ошибка при обработке данных: {e}")
        return []
        

def isna(x):
    if x is None:
        return True
    if isinstance(x, float):
        return math.isnan(x)        
    return False


def parse_slurm_time(time_str: str) -> Optional[datetime]:
    """
    Парсит время в формате SLURM в datetime объект
    Форматы: YYYY-MM-DDTHH:MM:SS, MM-DD HH:MM:SS, HH:MM:SS, N/A, Unknown
    """
    if isna(time_str) or time_str in ['N/A', 'Unknown', 'nan', '']:
        return None
    
    try:
        # Полный формат с датой: 2024-01-15T14:30:00
        if 'T' in time_str:
            return datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S')
        
        # Формат MM-DD HH:MM:SS (предполагаем текущий год)
        if '-' in time_str and ':' in time_str:
            current_year = datetime.now().year
            return datetime.strptime(f"{current_year}-{time_str}", '%Y-%m-%d %H:%M:%S')
        
        # Формат только времени HH:MM:SS (предполагаем сегодняшний день)
        if ':' in time_str and len(time_str.split(':')) == 3:
            today = datetime.now().date()
            time_part = datetime.strptime(time_str, '%H:%M:%S').time()
            return datetime.combine(today, time_part)
        
        return None
        
    except ValueError as e:
        print(f"Не удалось распарсить время '{time_str}': {e}")
        return None

def parse_nodes_list(nodes_str: str) -> List[str]:
    """
    Парсит строку с узлами в список отдельных узлов
    Примеры: "node[01-03,05]" -> ["node01", "node02", "node03", "node05"]
    """
    if isna(nodes_str) or nodes_str in ['N/A', 'Unknown', 'nan', '']:
        return []
    
    nodes = []
    
    # Обрабатываем формат node[01-03,05]
    pattern = r'(\w+)\[([^\]]+)\]'
    match = re.search(pattern, nodes_str)
    
    if match:
        prefix = match.group(1)
        ranges_str = match.group(2)
        
        for part in ranges_str.split(','):
            if '-' in part:
                # Диапазон узлов
                start, end = part.split('-')
                start_num = int(start)
                end_num = int(end)
                width = len(start)  # Для сохранения ведущих нулей
                
                for i in range(start_num, end_num + 1):
                    node_name = f"{prefix}{i:0{width}d}"
                    nodes.append(node_name)
            else:
                # Отдельный узел
                if part.isdigit():
                    width = len(part)
                    node_name = f"{prefix}{int(part):0{width}d}"
                    nodes.append(node_name)
                else:
                    nodes.append(f"{prefix}{part}")
    else:
        # Простой формат или список через запятую
        if ',' in nodes_str:
            nodes = [node.strip() for node in nodes_str.split(',')]
        else:
            nodes = [nodes_str.strip()]
    
    return [node for node in nodes if node]
    
# input: df это список словарей: [ {jobinfo}, {jobinfo}, ... ]
# output: gnodes это словарь хостов {hostname: {...}}
# output: user_tasks это список id задач выбранного пользователя, словарь вида
#         {"running":[...],"pending":[...],"other":[...]}
def build_hourly_schedule(df, gnodes, user_tasks):
    """
    Строит словарь расписания по часам
    """
    schedule = defaultdict(set)  # Используем set для избежания дубликатов
    now_time = datetime.now()
    
    #print(f"Текущее время: {current_time}")
    #print(f"Анализируем {len(df)} задач...")
    
    processed_jobs = 0
    
    max_time_slots = TIME_SLOTS
    
    for n in gnodes.keys():
      # битовая маска занятости
      sch = [0 for x in range(max_time_slots)]
      gnodes[n]['schedule'] = sch

      # колво занятых цпу
      cc = [0 for x in range(max_time_slots)]
      gnodes[n]['cpuinfo'] = cc

      #jobinfo = 
      # информация юзер:jobid
      gnodes[n]['jobinfo'] = [[] for x in range(max_time_slots)]
      
      # метки времени
      gnodes[n]['timeinfo'] = ["" for x in range(max_time_slots)]

    
    for idx, row in enumerate(df):
        try:
            start_time = parse_slurm_time(row.get('START_TIME', ''))
            end_time = parse_slurm_time(row.get('END_TIME', ''))
            nodes_str = row.get('SCHEDNODES', '(null)')
            if nodes_str == '(null)':
                nodes_str = row.get('NODELIST', '') 
            
            if not nodes_str or nodes_str in ['N/A', 'Unknown']:
                continue
            
            nodes = parse_nodes_list(nodes_str)
            if not nodes:
                #print("no nodes",nodes_str,row.get('SCHEDNODES', '(null)'),row.get('NODELIST', '-'))
                #print(row)
                # какие-то неназначенные задания
                continue
                
            #print(nodes)

            
            if HIDE_USERS: #F-HIDE-USERS
                # заменяем пользователя на имя программы
                #cmd = row.get('COMMAND','')
                #if cmd == '(null)':
                #    cmd = row.get('WORK_DIR','')
                #if cmd == '(null)':
                #    cmd = row.get('NAME','') #jobname
                #user = cmd.split("/")[-1]
                user = row.get('NAME','') #jobname она там всегда похоже есть и сразу какая надо
            else:
                user = row.get('USER','')

            jobid = str(row.get('JOBID',''))
            job_record = [user, jobid] # сюда запишем пользователя и айди работы
            
            # Если нет времени начала, используем текущее время для запущенных задач
            #if start_time is None:
            if row.get('STATE', '') == 'RUNNING':
                start_time = now_time
                sval = 4
            elif row.get('STATE', '') == 'PENDING':
                sval = 2
            else:
                sval = 1
                
            # вообще они бывают одновременно и running и pending это видимо если процессоры свободные есть
            #if row.get('STATE', '') == 'PENDING':
            #    sval = sval | 2
                
            if row.get('USER','') == HILITE_USER: #F-HILITE-USER-TASKS
                sval = sval | 8
                #F-SHOW-USER-TASKS
                if sval & 4:                    
                    user_tasks["running"].append( jobid )
                elif sval & 2:
                    user_tasks["pending"].append( jobid )
                else:
                    # todo тут может быть разбивка - ошибки и пр
                    user_tasks["other"].append( jobid )
            
            # Если нет времени окончания, пропускаем
            if end_time is None:
                #print("no end time")
                continue
            
            # Генерируем часы от начала до конца выполнения задачи
            
            # час начала задачи
            current_hour = start_time.replace(minute=0, second=0, microsecond=0)
            # час конца задачи
            end_hour = end_time.replace(minute=0, second=0, microsecond=0)
            
            # час сейчас
            start_hour = now_time.replace(minute=0, second=0, microsecond=0)

            if DETAILED_USAGE:
                #print("get details jobid", jobid, file=sys.stderr,flush=True)
                h = gather_for_jobids( [jobid] )
                jinfo = h[ jobid ]
            else:
                jinfo = {}
            
            while current_hour <= end_hour:
                diff = current_hour - start_hour
                hours = diff.total_seconds() / 3600
                time_slot = int(hours)
                #hour_key = current_hour.strftime('%Y-%m-%d %H:00')
                hour_key = current_hour.strftime('%d-%m-%Y %H:00')
                
                if time_slot < TIME_SLOTS:                
                    #print( time_slot )
                    for n in nodes:
                        sch = gnodes[n]['schedule']
                        sch[ time_slot ] = sch[ time_slot ] | sval
                        
                        sch_jobinfo = gnodes[n]['jobinfo']
                        sch_jobinfo[ time_slot ].append( job_record )
                        
                        gnodes[n]['timeinfo'][ time_slot ] = hour_key

                        if DETAILED_USAGE:
                            if n in jinfo:
                                #    print("tt",jinfo[n])
                                qq = jinfo[n]['usedcpu']
                                gnodes[n]['cpuinfo'][time_slot] += qq
                            else:
                                #print("n is not in jinfo, =",jinfo,file=sys.stderr)
                                pass
                    
                
                #schedule[hour_key].update(nodes)
                current_hour += timedelta(hours=1)
                
                #if sval & 8:
                #   print(current_hour, time_slot)
                    

            processed_jobs += 1
            
        except Exception as e:
            print(f"Ошибка при обработке задачи {idx}: {e}" )
            traceback.print_exc()
            continue
    
    #print(f"Обработано задач: {processed_jobs}")
    
    # Конвертируем sets в списки и сортируем
    #result = {}
    #for hour, nodes_set in schedule.items():
    #    result[hour] = sorted(list(nodes_set))
    
    #return dict(sorted(result.items()))
    return None
    
# вставляет в массив arr через каждые k элементов элемент e
# это нужно чтобы делать красивые колонки по k часов (тайм слотов)
# shift = тема #F-CURHOUR-SHIFT
def insert_every_k(arr, k, e, shift):
    result = []
    for i, num in enumerate(arr):
        result.append(num)
        if (i + 1 + shift) % k == 0 and i + 1 < len(arr):
            result.append(e)
    return result    
            
# gnodes - список узлов { узел : {schedule: ...} }
# печатает в текстовом режиме
def paint_text( gnodes, user_tasks ):

    if len(gnodes.keys()) == 0:
        return

    now_time = datetime.now() # todo вынести в параметр
    start_hour_i = now_time.hour
    #print("start hour=",start_hour_i)
    #print(f"Анализируем {len(df)} задач...") xxx

    # Text colors
    RED = '\033[31m'
    GREEN = '\033[32m'
    BLUE = '\033[34m'
    YELLOW = '\033[33m'
    CYAN = '\033[36m'    
    WHITE = '\x1b[37m'
    MAGENTA = '\x1b[35m'    
    RESET = '\033[0m' # Resets color to default

    # Background colors
    ON_RED = '\033[41m'
    ON_GREEN = '\033[42m'
    ON_BLUE = '\033[44m'
    ON_MAGENTA = '\033[45m'
    ON_YELLOW = '\033[43m'
    ON_CYAN = '\033[46m'
    ON_WHITE = '\033[47m'

    # Styles
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'    
    

    max_name_len = max([len(x) for x in gnodes.keys()])
    counter = 0
    for n in gnodes.keys():
        #color = RED if (n.startswith('apollo') and int(n[6:]) >= 17) or n.startswith('tesla-') else RESET
        rec = gnodes[n]
        sch = rec['schedule']
        # колонки по часам
        sch = insert_every_k( sch, SLOT_ITEMS, 16, start_hour_i )
        
        jobinfo = rec['jobinfo']
        jobinfo = insert_every_k( jobinfo, SLOT_ITEMS, [], start_hour_i )
        
        txt = ''
        slot_index = 0 # номер слота = номер позиции в расписании (с учетом insert_every_k)
        hour_index = -1
        for x in sch:
            j = jobinfo[ slot_index ]
            c = '.'
            if x & 4: # running
                if SHOW_JOB_CNT:
                    if len(j) < 10 :
                        c = str(len(j)) # покажем число job-ов в слоте #F-JOB-CNT
                        #print("c=",c,file=sys.stderr)
                    else:
                        c = '+'
                else:
                    c = 'R'                
            elif x & 2: # pending
                c = '#'
            elif x & 1: #other
                c = '?'
            
            if x & 8: # hilite user
               #F-HILITE-USER-TASKS
               #c = ON_BLUE + c + RESET
               c = ON_RED + c + RESET # красный выбран потому что в веб-варианте там оранжевый               
               #print("x=",x)            
               
            if x & 16: # колонка по часам
               c = ' '
            else:
                hour_index += 1 # это реальная колонка а не пробел - увеличим час

            # #F-HILITE-DAY подсветим границу суток
            if (start_hour_i + hour_index) % 24 == 0:
               #c = GREEN + c + RESET 
               c = CYAN + c + RESET 

            txt += c
            slot_index += 1
            
        #print(len(txt))
        
        #mapping = {4: 'R', 0: '_', 2: '=', 1: '?'}
        #mapping = {4: '@', 0: '.', 2: '#', 1: '?'}
        #mapping = {4: 'R', 0: '.', 2: '#', 1: '?'}
        #result = ''.join([mapping[x] for x in sch])
        result = txt
        
        # разделяем на колонки по часам
        #result = re.sub(r'(.{6})', r'\1 ', result).strip()
        
        # разделяем на колонки по хостам
        eol = '\n' if counter % COLUMNS == (COLUMNS-1) else ' '
        counter += 1
        #print(n.rjust(max_name_len), result,end=eol)
        
        #n = n + " " + str(rec['cpus_free'])        
        
        cpu_info = (str(rec['cpus_free']) + "/" + str(rec['cpus_total']) ).rjust(5) # idle / total
        #F-NODE-NONBUSY-HILITE
        color = RESET
        if rec['state'] == "down*":
            color=RED
        elif rec['cpus_free'] == 0:
            color = RESET
        elif rec['cpus_free'] >= rec["cpus_total"]/3:
            color = GREEN #CYAN #RED # недозагрузка
        else:
            color = YELLOW #CYAN #RED # RESET # пустые и так видно
        print(color,n.rjust(max_name_len), cpu_info,  RESET, result,end=eol)

    print("Легенда: Имя узла, свободно-cpu/всего-cpu, 1..9+ = число задач на узле, # = запланировано. ",end="")
    print("Колонка - час. ", end="")
    print(CYAN+"Голубая полоса"+RESET+" - граница суток.")
    print("Серый - узел загружен полностью. ", end="")
    print(YELLOW+"Жёлтый"+RESET+"/"+GREEN+"Зелёный"+RESET+" - узел загружен частично или свободен. ", end="")
    print(RED+"Красный"+RESET+" - узел выключен.")
    print(ON_RED+f"Красный фон"+RESET+" - задачи",HILITE_USER, end="")
    if len(user_tasks["running"]):
        print(" работают:"," ".join(user_tasks["running"]), end="" ) 
    if len(user_tasks["pending"]):
        print(" ожидают:"," ".join(user_tasks["pending"]), end="" )
    if len(user_tasks["other"]):
        print(" неясные:"," ".join(user_tasks["other"]), end="" )
    #print(" см.",ON_RED,"mqinfo | grep ",HILITE_USER,RESET)
    print(" подробности:",BOLD,"mqinfo | grep",HILITE_USER,RESET)
    #F-SHOW-USER-TASKS
    print("Текущее время:",now_time.strftime('%d-%m-%Y %H:%M'))
    #print()
    
# gnodes - список узлов { узел : {schedule: ...} }
# где schedule это числовой массив
def paint_html( gnodes ):

    now_time = datetime.now() # todo вынести в параметр
    start_hour_i = now_time.hour
    total_users = dict() # username => 1
    max_name_len = max([len(x) for x in gnodes.keys()]) if len(gnodes) > 0 else 10
    counter = 0
    
    RES = ""
    #F-AUTO-COLS сделано через стили css grid и вложенный grid для информации по узлу
    
    for n in gnodes.keys():
        #color = RED if (n.startswith('apollo') and int(n[6:]) >= 17) or n.startswith('tesla-') else RESET
        rec = gnodes[n]
        sch = rec['schedule']
        # колонки по часам
        sch = insert_every_k( sch, SLOT_ITEMS, 16,start_hour_i )

        jobinfo = rec['jobinfo']
        jobinfo = insert_every_k( jobinfo, SLOT_ITEMS, [],start_hour_i )
        
        timeinfo = rec['timeinfo']
        timeinfo = insert_every_k( timeinfo, SLOT_ITEMS, "",start_hour_i )

        # занятость процессоров
        cpuinfo = rec['cpuinfo']
        cpuinfo = insert_every_k( cpuinfo, SLOT_ITEMS, "",start_hour_i )
        
        txt = ''
        slot_index = 0
        hour_index = -1
        
        for x in sch:
            j = jobinfo[ slot_index ]
            c = '.'
            cl="cell uelem"
            if x & 4: # running
                if SHOW_JOB_CNT:
                    if len(j) < 10 :
                        c = str(len(j)) # покажем число job-ов в слоте #F-JOB-CNT
                        #print("c=",c,file=sys.stderr)
                    else:
                        c = '+'
                else:
                    c = 'R'
                cl += " running"
            elif x & 2: # pending
                c = '#'
                cl += " pending"
            elif x & 1: #other
                c = '?'
            else:
                cl += " free"
                
            if x & 8: # hilite user
               #c = ON_BLUE + c + RESET
               #print("x=",x)
               cl += " user"
               pass
               
            if x & 16: # колонка по часам
               #c = '&nbsp;'
               c = ''
               cl += " timeslot"
            else:
                hour_index += 1 # это реальная колонка а не пробел - увеличим час

            # #F-HILITE-DAY подсветим границу суток
            if (start_hour_i + hour_index) % 24 == 0:
               cl += " hilite_day"

            #if (start_hour_i + slot_index) % 24 == 0:
            #   c = GREEN + c + RESET    

            # jobinfo - выведем подробную информацию

            #print(j)
            title = ""

            t = timeinfo[ slot_index ]
            title += html.escape(t) + "&#10;"

            for item in j:
                user = item[0]
                jobid = item[1]
                # Экранируем данные для использования в CSS классах (только буквы, цифры, дефис, подчеркивание)
                user_safe = re.sub(r'[^a-zA-Z0-9_-]', '_', user)
                jobid_safe = re.sub(r'[^a-zA-Z0-9_-]', '_', str(jobid))
                cl += ' user_' + user_safe
                #F-TOOLTIP
                title += html.escape(user) + ":" + html.escape(str(jobid)) + "&#10;"
                total_users[user] = 1
                #F-HILITE-USERJOB
                cl += ' job_' + jobid_safe

            if DETAILED_USAGE:
                cc = cpuinfo[ slot_index ]
                title += "cpus: " + html.escape(str(cc))

            txt += "<div class='" + html.escape(cl) + "' title='" + title + "'>" + html.escape(c) + "</div>\n"
            slot_index += 1
            
        #print(len(txt))
        
        #mapping = {4: 'R', 0: '_', 2: '=', 1: '?'}
        #mapping = {4: '@', 0: '.', 2: '#', 1: '?'}
        #mapping = {4: 'R', 0: '.', 2: '#', 1: '?'}
        #result = ''.join([mapping[x] for x in sch])
        result = txt
        
        # разделяем на колонки по часам
        #result = re.sub(r'(.{6})', r'\1 ', result).strip()
        
        # разделяем на колонки по хостам
        #eol = '\n' if counter % COLUMNS == (COLUMNS-1) else ' '

        #print(n.rjust(max_name_len), result,end=eol)
        
        #n = n + " " + str(rec['cpus_free'])        
        
        cpu_info = (str(rec['cpus_free']) + "/" + str(rec['cpus_total']) ).rjust(5) # idle / total

        #F-NODE-NONBUSY-HILITE
        node_usage_class = "nodenonbusy"
        if rec['state'] == "down*":
            node_usage_class = "nodedown"
        elif rec['cpus_free'] == 0:
            node_usage_class = "nodebusy"
        elif rec['cpus_free'] == rec["cpus_total"]:
            node_usage_class = "nodefree"
        elif rec['cpus_free'] < rec["cpus_total"]/3:
            #F-NONBUSY-PARTIAL все что меньше трети делаем не такое яркое
            node_usage_class = "nodebusy60"
        else:
            node_usage_class = "nodebusy30"

        #print(color,n.rjust(max_name_len), cpu_info,  RESET, result,end=eol)
        #if counter % COLUMNS == 0:
        #    print(ROW_BEGIN)
        #print("<div class='node'><div class='nodename'>",n,"</div><div class='cpuinfo'>",cpu_info,"</div>",result,"</div>")
        RES += "<div class='node'><div class='nodename'>" + html.escape(n) + "</div><div class='cpuinfo " + html.escape(node_usage_class) + "'>" + html.escape(cpu_info) + "</div>" + result + "</div>"
        #if counter % COLUMNS == COLUMNS-1:
    #        print("</tr>")
        counter += 1        
        

    USERS = ""
    total_users[" "] = 1
    for name in sorted( list(total_users.keys()) ):
        # Экранируем имя для CSS класса
        name_safe = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
        t = "<div class='userinfo uelem user_" + name_safe + "'>" + html.escape(name) + "</div>\n"
        USERS += t
    
    #F-CURTIME
    now_time_s = datetime.now().strftime('%d-%m-%Y %H:%M')    

    return [RES, USERS, now_time_s]

# загрузить шаблон, завернуть в него строку block[0], напечатать на экран
def use_template( block ):
    # вариант чтения из файла
    script_dir = Path(__file__).resolve().parent

    tpl_path_env = os.environ.get('TEMPLATE','')
    if len(tpl_path_env) == 0:
        tpl_path = script_dir / 'mqvis_template.html'
    else:
        # Защита от path traversal - проверяем, что путь находится в разрешенной директории
        tpl_path = Path(tpl_path_env).resolve()
        try:
            # Проверяем, что путь находится внутри директории скрипта
            tpl_path.relative_to(script_dir)
        except ValueError:
            sys.exit(f'Error: template path must be within script directory. Got: {tpl_path}')

    if not tpl_path.exists():
        sys.exit(f'Error: template not found: {tpl_path}')

    result = tpl_path.read_text(encoding='utf-8')

    # заменить все вхождения PLACEHOLDER (для замены только первого: .replace('PLACEHOLDER', block, 1))
    result = result.replace('PUT_TABLE', block[0])
    result = result.replace('PUT_USERS', block[1])
    result = result.replace('PUT_TIME', block[2])

    # вывести в stdout в UTF-8
    sys.stdout.buffer.write(result.encode('utf-8'))

###########################################

# Использование
nodes_dict = simple_sinfo_dict()

df= get_jobs_dataframe()
#fdf = df.loc[df['STATE'] == 'RUNNING']
#print(fdf)

user_tasks={"running":[],"other":[],"pending":[]}
build_hourly_schedule(df, nodes_dict, user_tasks)
# nodes_dict после build_hourly_schedule содержит {node: {schedule:..., jobinfo: ..., timeinfo: ... }}
# где schedule это массив с битовыми масками, jobinfo список пользователей и задач, timeinfo время

#print(json.dumps(nodes_dict, indent=2, ensure_ascii=False))

if FORMAT == "html":
    use_template( paint_html( nodes_dict ) )
else:
    #print(HILITE_USER)
    paint_text( nodes_dict, user_tasks )

# done
