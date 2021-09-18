import yaml
import psutil
import pandas as pd
import datetime

from sys_poll import Sys_poll

__author__ = "StevenGuarino"
__version__ = "0.1"

"""
-database exists when created DONE
-create table, check if exists DONE
-insert, select make sure it exists DONE
-delete from table, and confirm DONE
-drop table, make sure dropped DONE
-drop db, confirm dropped DONE
-type check pids are int DONE
-type check process objs are Process DONE
-check values are in range DONE
"""

def test_db_created():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  assert(sys_poll_obj.poll_db.check_db_exists(sys_poll_obj.config_args["db_name"]))
# end

def test_table_created():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  cols = "test_col varchar(255)"
  sys_poll_obj.poll_db.create_table(sys_poll_obj.table_names, cols)
  assert(sys_poll_obj.poll_db.check_table_exists(sys_poll_obj.table_names))
# end

def test_table_insert():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  pid = list(map(int, sys_poll_obj.get_processes()[0]))
  process_obj = psutil.Process(pid[0])
  all_process_metric = sys_poll_obj.get_process_metrics([process_obj,
                                                          sys_poll_obj.metrics])
  cols = [] # TODO add unique identifier
  keys = [] # TODO add unique identifier
  for key in all_process_metric.keys():
    keys.append(key)
    if key == "nowtime": cols.append(str(key) + " datetime")
    else: cols.append(str(key) + " varchar(255)")
  cols = ", ".join(cols)
  vals = [all_process_metric[k] for k in keys]

  sys_poll_obj.poll_db.create_table(sys_poll_obj.table_names,
                                    cols) # create current table
  sys_poll_obj.poll_db.insert_into_table(sys_poll_obj.table_names,
                                         ", ".join(keys),
                                         [vals])
  results = sys_poll_obj.poll_db.select_from_table(sys_poll_obj.table_names,
                                                   keys)[0]
  idx_of_datetime = [type(x) for x in results].index(datetime.datetime)
  vals[idx_of_datetime] = str(vals[idx_of_datetime]).split(".")[0]
  assert(list(map(str, results))==list(map(str, vals)))
# end

def test_delete_from_table():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  pid = list(map(int, sys_poll_obj.get_processes()[0]))
  process_obj = psutil.Process(pid[0])
  all_process_metric = sys_poll_obj.get_process_metrics([process_obj,
                                                          sys_poll_obj.metrics])
  cols = [] # TODO add unique identifier
  keys = [] # TODO add unique identifier
  for key in all_process_metric.keys():
    keys.append(key)
    if key == "nowtime": cols.append(str(key) + " datetime")
    else: cols.append(str(key) + " varchar(255)")
  cols = ", ".join(cols)
  vals = [all_process_metric[k] for k in keys]

  sys_poll_obj.poll_db.create_table(sys_poll_obj.table_names,
                                    cols) # create current table
  sys_poll_obj.poll_db.insert_into_table(sys_poll_obj.table_names,
                                         ", ".join(keys),
                                         [vals])
  before_results_id = sys_poll_obj.poll_db.select_from_table(sys_poll_obj.table_names,
                                                      ["id"])
  sys_poll_obj.poll_db.delete_by_id_from_table(sys_poll_obj.table_names,
                                               "id",
                                               before_results_id[0][0])
  after_results_id = sys_poll_obj.poll_db.select_from_table(sys_poll_obj.table_names,
                                                            ["id"])
  assert(before_results_id[0][0] not in after_results_id)
# end

def test_db_dropped():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  sys_poll_obj.poll_db._drop_db(sys_poll_obj.config_args["db_name"])
  assert(not sys_poll_obj.poll_db.check_db_exists(sys_poll_obj.config_args["db_name"]))
# end

def test_table_dropped():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  cols = "test_col varchar(255)"
  sys_poll_obj.poll_db.create_table(sys_poll_obj.table_names, cols)
  sys_poll_obj.poll_db._drop_table(sys_poll_obj.table_names)
  assert(not sys_poll_obj.poll_db.check_table_exists(sys_poll_obj.table_names))
# end

def test_id_is_int():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  pid = list(map(int, sys_poll_obj.get_processes()[0]))
  process_obj = psutil.Process(pid[0])
  all_process_metric = sys_poll_obj.get_process_metrics([process_obj,
                                                          sys_poll_obj.metrics])
  cols = [] # TODO add unique identifier
  keys = [] # TODO add unique identifier
  for key in all_process_metric.keys():
    keys.append(key)
    if key == "nowtime": cols.append(str(key) + " datetime")
    else: cols.append(str(key) + " varchar(255)")
  cols = ", ".join(cols)
  vals = [all_process_metric[k] for k in keys]

  sys_poll_obj.poll_db.create_table(sys_poll_obj.table_names,
                                    cols) # create current table
  sys_poll_obj.poll_db.insert_into_table(sys_poll_obj.table_names,
                                         ", ".join(keys),
                                         [vals])
  before_results_id = sys_poll_obj.poll_db.select_from_table(sys_poll_obj.table_names,
                                                      ["id"])
  assert(type(before_results_id[0][0]) is int)
# end

def test_pid_is_int():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  pid = list(map(int, sys_poll_obj.get_processes()[0]))
  process_obj = psutil.Process(pid[0])
  all_process_metric = sys_poll_obj.get_process_metrics([process_obj,
                                                          sys_poll_obj.metrics])
  assert(type(pid[0]) is int)
# end

def test_metrics_memory_percent_normalized():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  pid = list(map(int, sys_poll_obj.get_processes()[0]))
  process_obj = psutil.Process(pid[0])
  all_process_metric = sys_poll_obj.get_process_metrics([process_obj,
                                                          sys_poll_obj.metrics])
  if "memory_percent" in all_process_metric.keys():
    assert(all_process_metric["memory_percent"] <= 1.0 or all_process_metric["memory_percent"] > 0)
# end

def test_metrics_cpu_percent_normalized():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  pid = list(map(int, sys_poll_obj.get_processes()[0]))
  process_obj = psutil.Process(pid[0])
  all_process_metric = sys_poll_obj.get_process_metrics([process_obj,
                                                          sys_poll_obj.metrics])
  if "cpu_percent" in all_process_metric.keys():
    assert(all_process_metric["cpu_percent"] <= 1.0 or all_process_metric["cpu_percent"] > 0)
# end

def test_metrics_min_num_threads():
  args = yaml.safe_load(open("test_sys_poll.yml", "r"))
  sys_poll_obj = Sys_poll(args) # will create db
  pid = list(map(int, sys_poll_obj.get_processes()[0]))
  process_obj = psutil.Process(pid[0])
  all_process_metric = sys_poll_obj.get_process_metrics([process_obj,
                                                          sys_poll_obj.metrics])
  if "num_threads" in all_process_metric.keys():
    assert(all_process_metric["num_threads"] >= 1)
# end
