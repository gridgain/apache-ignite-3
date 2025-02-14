#!/usr/bin/env python3

import subprocess
import os
import shutil
import datetime
import dataclasses

def run_bench(data_dir, work_dir, run_dir, file_prefix, cmd_env):
    gc_log_file = f"{run_dir}/{file_prefix}_gc.log"
    print(gc_log_file)

    with open(current_file, "t+w") as f:
        command = [
        "./gradlew", ":ignite-sql-engine:integrationTest",
        "--tests", "*ItVarlenTypesTest.testKv",
        f"-DjvmLogOpts=-Xlog:gc=debug:file={gc_log_file}:time,uptime,tid,level,tags"
        ]

        print(f"Command {command}")

        try:
            shutil.rmtree(f"{data_dir}/cluster")
        except FileNotFoundError:
            print(f"Test data dir does not exist: {data_dir}/cluster")

        process = subprocess.run(command, cwd=work_dir, env=cmd_env, stdout=f, stderr=f, text=True)
        print(f"Exit code: {process.returncode}")
        print()

        return process.returncode


def kill_gradle():
    # Gradle keeps spawning its worker processes like there is no tomorrow
    os.system("pkill -f GradleDaemon")


work_dir = "/Users/max/Projects/java/ignite-3"
data_dir = f"{work_dir}/test_cluster"
# data_dir = "/Volumes/WorkDisk/Users/mzhuravkov/test_data"

now = datetime.datetime.now()
run = now.strftime("%Y_%m_%d_%H_%M_%S")
run_dir = f"{work_dir}/benches/run_{run}_3_nodes"

report_file = f"{run_dir}/report.txt"
stop_file = f"{work_dir}/benches/stop"

print(f"Work dir: {work_dir}")
print(f"Data dir: {data_dir}")
print(f"Run dir: {run_dir}")
print(f"Report file: {report_file}")
print(f"Stop file: {stop_file}")
print()

os.mkdir(run_dir)

try:
    os.remove(stop_file)
    print(f"Removed a stop file: {stop_file}")
except FileNotFoundError:
    pass

@dataclasses.dataclass
class Config:
    # STRING, BINARY
    tpe: str
    # KEY, VALUE
    mode: str
    # 1_m, 2_m
    record_size: str
    # 1_g, 512_m
    record_size: str
    # 1_g, 512_m
    region_size: str
    # writers
    num_writers: int
    # readers
    num_readers: int

    def clone(self, mode=None, record_size=None, region_size=None, num_writers=1, num_readers=None):
        copy = Config(self.tpe, self.mode, self.record_size, self.region_size, self.num_writers, self.num_readers)

        if mode is not None:
          copy.mode = mode

        if record_size is not None:
          copy.record_size = record_size

        if region_size is not None:
            copy.region_size = region_size

        if num_writers is not None:
            copy.num_writers = num_writers

        if num_readers is not None:
            copy.num_readers = num_readers

        return copy

    def file_name(self):
        return f"{self.tpe}_mode_{self.mode}_size_{self.record_size}_region_size_{self.region_size}_w{self.num_writers}_r{self.num_readers}"


string_cfg = Config(tpe="STRING", mode="VALUE", record_size="10_m", region_size="512_m", num_writers=1, num_readers=0)
binary_cfg = Config(tpe="BINARY", mode="VALUE", record_size="10_m", region_size="512_m", num_writers=1, num_readers=0)

benches = [
# STRING
#   10M | STRING_mode_VALUE_size_20_m_region_size_1_g_w1_r1.log <
  string_cfg.clone(record_size="20_m", region_size="512_m", num_writers=1, num_readers=1),
#  3.7M | STRING_mode_VALUE_size_20_m_region_size_512_m_w2_r1.log
  string_cfg.clone(record_size="20_m", region_size="1_g", num_writers=2, num_readers=1),
#   18M | STRING_mode_VALUE_size_30_m_region_size_1_g_w2_r1.log
  string_cfg.clone(record_size="30_m", region_size="512_m", num_writers=1, num_readers=1),
#   24M | STRING_mode_VALUE_size_30_m_region_size_512_m_w1_r1.log
#  3.5M | STRING_mode_VALUE_size_30_m_region_size_512_m_w2_r1.log
#   11M | STRING_mode_VALUE_size_40_m_region_size_1_g_w2_r1.log
#  7.3M | STRING_mode_VALUE_size_40_m_region_size_512_m_w1_r1.log
#   11M | STRING_mode_VALUE_size_40_m_region_size_512_m_w2_r1.log
#   24M | STRING_mode_VALUE_size_50_m_region_size_1_g_w2_r1.log
#   32M | STRING_mode_VALUE_size_50_m_region_size_512_m_w1_r1.log
#  400M | STRING_mode_VALUE_size_50_m_region_size_512_m_w2_r1.log
  string_cfg.clone(record_size="50_m", region_size="512_m", num_writers=2, num_readers=1),
# BINARY
#  236M | BINARY_mode_VALUE_size_20_m_region_size_512_m_w1_r1.log
  binary_cfg.clone(record_size="20_m", region_size="512_m", num_writers=1, num_readers=1),
#   16M | BINARY_mode_VALUE_size_30_m_region_size_512_m_w1_r1.log
#   16M | BINARY_mode_VALUE_size_30_m_region_size_512_m_w2_r1.log
#   18M | BINARY_mode_VALUE_size_40_m_region_size_1_g_w2_r1.log
#   13M | BINARY_mode_VALUE_size_40_m_region_size_512_m_w2_r1.log
#   29M | BINARY_mode_VALUE_size_50_m_region_size_1_g_w2_r1.log
#  424M | BINARY_mode_VALUE_size_50_m_region_size_512_m_w1_r1.log
  binary_cfg.clone(record_size="50_m", region_size="512_m", num_writers=1, num_readers=1),
#  140M | BINARY_mode_VALUE_size_50_m_region_size_512_m_w2_r1.log <
  binary_cfg.clone(record_size="50_m", region_size="512_m", num_writers=2, num_readers=1)
]


record_sizes = ["25_m", "30_m", "35_m", "40_m", "45_m", "50_m"]
record_sizes = ["5_m", "10_m", "15_m"]
region_sizes = ["512_m", "1_g"]
writers = [1]

benches = []
for m in ["VALUE"]:
  for sz in record_sizes:
    for rz in region_sizes:
      for n in writers:
        benches.append(string_cfg.clone(mode=m, record_size=sz, region_size=rz, num_writers=n, num_readers=1))
        benches.append(binary_cfg.clone(mode=m, record_size=sz, region_size=rz, num_writers=n, num_readers=1))

print("Benches", len(benches))

for b in benches:
   print(b)

print()

# exit(1)

duration_time="10_m"

for cfg in benches:
    cmd_env = os.environ.copy()
    cmd_env["TEST_CLUSTER_SIZE"] = "3"
    cmd_env["TEST_MAX_DATA_SIZE_GB"] = "150"
    cmd_env["TEST_DATA_DIR"] = data_dir
    cmd_env["TEST_RECORD_SIZE"] = f"{cfg.record_size}"
    cmd_env["TEST_DURATION"] = duration_time
    cmd_env["TEST_NUM_WRITERS"] = f"{cfg.num_writers}"
    cmd_env["TEST_NUM_READERS"] = f"{cfg.num_readers}"
    cmd_env["TEST_DATA_REGION_SIZE"] = f"{cfg.region_size}"
    cmd_env["IGNITE_CI"] = "true"

    current_dir = f"{work_dir}/benches/current"
    current_file = f"{run_dir}/current.log"

    # To ease monitoring
    if os.path.exists(current_dir):
        os.remove(current_dir)

    os.symlink(run_dir, current_dir)

    # Stop, if a stop file exists
    if os.path.exists(stop_file):
        print(f"Stop file exists: {stop_file}. Done")
        break

    file_prefix = cfg.file_name()

    print(f"Run: {cfg}")
    print(f"File prefix: {file_prefix}")

    return_code = run_bench(data_dir, work_dir, current_dir, file_prefix, cmd_env)

    kill_gradle()

    dst_file = f"{run_dir}/{file_prefix}.log"
    shutil.move(current_file, dst_file)

    # print(f"Output file: {dst_file}")

    with open(report_file, "a") as f:
        if return_code == 0:
            rs = "SUCCESS"
        else:
            rs = "FAILURE"
        report_item = f"{cfg} {rs}\n"
        f.write(report_item)


# Print report file
print("Results:")
with open(report_file, "r") as f:
    for line in f.readlines():
        print(line, end="")
