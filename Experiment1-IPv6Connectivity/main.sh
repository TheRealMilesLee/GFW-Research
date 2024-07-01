#!/bin/bash

# 输入文件包含网站列表
input_file="websites.txt"
# 输出文件包含网站访问情况
output_file="connectivity_results.dat"
# 定时间隔（秒）
interval=3600 # 每小时检测一次
# 循环检测次数
iterations=168 # 168次，表示一整周

# 创建或清空输出文件
echo -n "" >"$output_file"

# 记录头信息
echo "# Timestamp Website Status DayTime Period" >>"$output_file"

# 定义函数检测连通性
check_connectivity() {
	url=$1
	if curl -6 --silent --head --fail "$url" >/dev/null; then
		echo "Success"
	else
		echo "Failure"
	fi
}

# 循环检测
for ((i = 0; i < iterations; i++)); do
	timestamp=$(date "+%Y-%m-%d %H:%M:%S")
	day_of_week=$(date "+%u") # 1 (Monday) to 7 (Sunday)
	hour_of_day=$(date "+%H") # 00 to 23

	if [[ $day_of_week -lt 6 ]]; then
		day_type="Weekday"
	else
		day_type="Weekend"
	fi

	if [[ $hour_of_day -ge 8 && $hour_of_day -lt 20 ]]; then
		period="Day"
	else
		period="Night"
	fi

	while IFS= read -r website; do
		status=$(check_connectivity "$website")
		echo "$timestamp $website $status $day_type $period" >>"$output_file"
	done <"$input_file"
	sleep "$interval"
done
