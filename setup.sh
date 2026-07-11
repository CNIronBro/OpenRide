#!/usr/bin/env bash
# 一键启动脚本
set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Step 1: 启动 Docker 中间件
echo -e "${YELLOW}[1/4]${NC} 启动 Docker 中间件（MySQL + Redis + RabbitMQ + xxl-job）..."
docker-compose up -d

# Step 2: 等待 MySQL 就绪
echo -e "${YELLOW}[2/4]${NC} 等待 MySQL 就绪..."
until docker exec didi-mysql mysqladmin ping -h localhost -uroot -proot --silent 2>/dev/null; do
    echo -n "."
    sleep 2
done
echo ""
echo -e "  ${GREEN}✓${NC} MySQL 已就绪"

# Step 3: 初始化 xxl-job 表结构
echo -e "${YELLOW}[3/4]${NC} 检查 xxl-job 表结构..."
TABLE_COUNT=$(docker exec didi-mysql mysql -uroot -proot -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='xxl_job'" 2>/dev/null)
if [ "$TABLE_COUNT" -eq 0 ]; then
    echo "  正在下载 xxl-job 官方建表 SQL..."
    XXL_SQL_URL="https://raw.githubusercontent.com/xuxueli/xxl-job/master/doc/db/tables_xxl_job.sql"
    if command -v curl &> /dev/null; then
        curl -sL "$XXL_SQL_URL" -o /tmp/tables_xxl_job.sql
    elif command -v wget &> /dev/null; then
        wget -q "$XXL_SQL_URL" -O /tmp/tables_xxl_job.sql
    else
        echo -e "  ${YELLOW}⚠${NC} 未找到 curl 或 wget，请手动下载："
        echo "     $XXL_SQL_URL"
        echo "     然后执行: docker exec -i didi-mysql mysql -uroot -proot xxl_job < tables_xxl_job.sql"
        exit 1
    fi
    docker exec -i didi-mysql mysql -uroot -proot xxl_job < /tmp/tables_xxl_job.sql
    rm -f /tmp/tables_xxl_job.sql
    echo -e "  ${GREEN}✓${NC} xxl-job 表结构已初始化"
else
    echo -e "  ${GREEN}✓${NC} xxl-job 表已存在，跳过"
fi

# Step 4: 启动 Spring Boot 应用
echo -e "${YELLOW}[4/4]${NC} 启动 Spring Boot 应用..."
echo ""
echo -e "${GREEN}${BOLD}  ✓ 环境准备完成！请手动启动应用：${NC}"
echo ""
echo "    mvn clean spring-boot:run"
echo ""
echo -e "  ${CYAN}访问地址：${NC}"
echo "    乘客端：   http://localhost:8080/passenger/index.html"
echo "    司机端：   http://localhost:8080/driver/index.html"
echo "    管理员端： http://localhost:8080/admin/index.html"
echo "    RabbitMQ： http://localhost:15672 (guest / guest)"
echo "    xxl-job：  http://localhost:8090/xxl-job-admin (admin / 123456)"
echo ""
echo -e "  ${YELLOW}⚠ 启动应用后，请在 xxl-job 调度中心配置执行器和任务（见 README）${NC}"
echo ""