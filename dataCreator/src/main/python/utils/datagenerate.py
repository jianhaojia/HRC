import json
import random
import time

# 定义包装机的数量
NUM_PACKAGERS = 5

# 定义状态列表
STATUSES = ["running"]

# 定义时间格式
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# 定义数据生成函数
def generate_packager_data(packager_id, current_time, uptime):
    return {
        "id": packager_id,
        "time": current_time,
        "production": random.randint(5, 10),
        "status": random.choice(STATUSES),
        "uptime": uptime  # 使用传入的uptime值
    }

# 主函数
def main():
    # 初始化每台包装机的uptime为随机值
    uptimes = [random.randint(1200, 1440) for _ in range(NUM_PACKAGERS)]

    with open("F:/AI/HRC/dataCreator/src/main/resources/packager.txt", "a") as file:
        while True:
            current_time = time.strftime(TIME_FORMAT, time.gmtime())
            for packager_id in range(1, NUM_PACKAGERS + 1):
                data = generate_packager_data(packager_id, current_time, uptimes[packager_id - 1])
                json_data = json.dumps(data)
                file.write(json_data + "\n")
                file.flush()  # 确保数据立即写入文件
                # 更新uptime值
                uptimes[packager_id - 1] += 0.5
            time.sleep(30)  # 每30秒发送一次数据

# 运行主函数
if __name__ == "__main__":
    main()
