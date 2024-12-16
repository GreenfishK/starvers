from datetime import datetime
import os


def test_write_timing_info():
    path = "./evaluation/test/"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Persist Timings
    timestamp = int(datetime.timestamp(datetime.now()))
    with open(f"{path}test_timings.csv", "a+") as timing_file:
        timing_file.write(f"{timestamp}, {1}, {1}, {1}, {1}, {1}, {1}")
        timing_file.write('\n')

def test_wirte_delta_dump():
    path = "./evaluation/test/"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Persist Inserts, Deletions
    timestamp = int(datetime.timestamp(datetime.now()))
    with open(f"{path}/test_{timestamp}.delta", "a+") as dump_file:
        dump_file.writelines(map(lambda x: "- " + x + '\n', ['a', 'b', 'c']))
        dump_file.writelines(map(lambda x: "+ " + x + '\n', ['a', 'b', 'c']))
