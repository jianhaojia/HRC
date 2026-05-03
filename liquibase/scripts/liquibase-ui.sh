#!/bin/bash
# ============================================================================
# Liquibase ETL 管理工具 - 交互式菜单
# ============================================================================
#
# 使用场景:
#   - 开发环境: 写 changeset
#   - UAT 环境: 测试变更
#
# PRD 部署由 CI/CD 自动处理，不需要此工具
#
# ============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# 数据库配置
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-3306}
DB_NAME=${DB_NAME:-"etl_db"}
DB_USER=${DB_USER:-"liquibase"}
DB_PASS=${DB_PASS:-""}

# ============================================================================
# 工具函数
# ============================================================================

clear_screen() { clear; }

print_header() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC}        ${BOLD}Liquibase ETL 管理工具${NC}                              ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}                                                              ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}  数据库: ${DB_HOST}:${DB_PORT}/${DB_NAME}                      ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}                                                              ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}  ${YELLOW}开发 & UAT 专用 | PRD 由 CI/CD 自动部署${NC}                ${CYAN}║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

pause() {
    echo ""
    echo -e "${YELLOW}按任意键继续...${NC}"
    read -n 1 -s -r
}

read_db_config() {
    echo ""
    echo -e "${YELLOW}请输入数据库配置:${NC}"
    echo ""
    read -p "  数据库主机: " DB_HOST
    read -p "  端口: " DB_PORT
    read -p "  数据库名: " DB_NAME
    read -p "  用户名: " DB_USER
    read -s -p "  密码: " DB_PASS
    echo ""
}

# ============================================================================
# 菜单选项
# ============================================================================

menu_new_changeset() {
    echo ""
    echo -e "${BOLD}【NEW CHANGESET - 创建新变更文件】${NC}"
    echo ""

    echo "  1. mapper (ETLmapper 表)"
    echo "  2. master (流程定义表)"
    echo "  3. config (配置表)"
    echo ""
    read -p "  选择类型 (1/2/3): " choice

    case $choice in
        1) type="mapper" ;;
        2) type="master" ;;
        3) type="config" ;;
        *) echo "无效选择"; return ;;
    esac

    read -p "  描述: " description
    description=${description:-"new_changeset"}

    echo ""
    echo -e "${YELLOW}正在创建 changeset...${NC}"
    bash scripts/new_changeset.sh "$type" "$description"
    echo ""
    echo -e "${GREEN}✓ 创建完成!${NC}"
}

menu_update_uat() {
    echo ""
    echo -e "${BOLD}【UPDATE UAT - 部署到 UAT】${NC}"
    echo ""

    echo -e "${YELLOW}正在执行 liquibase:update --context=uat...${NC}"
    echo ""

    mvn liquibase:update \
        -Dliquibase.url="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
        -Dliquibase.username="$DB_USER" \
        -Dliquibase.password="$DB_PASS" \
        -Dliquibase.contexts=uat

    echo ""
    echo -e "${GREEN}✓ UAT 部署完成!${NC}"
}

menu_validate() {
    echo ""
    echo -e "${BOLD}【VALIDATE - 验证 changeset】${NC}"
    echo ""
    echo -e "${YELLOW}正在验证...${NC}"
    echo ""

    mvn liquibase:validate \
        -Dliquibase.url="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
        -Dliquibase.username="$DB_USER" \
        -Dliquibase.password="$DB_PASS"

    echo ""
    echo -e "${GREEN}✓ 验证通过!${NC}"
}

menu_status() {
    echo ""
    echo -e "${BOLD}【STATUS - 查看待执行变更】${NC}"
    echo ""
    mvn liquibase:status \
        -Dliquibase.url="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
        -Dliquibase.username="$DB_USER" \
        -Dliquibase.password="$DB_PASS"
}

menu_history() {
    echo ""
    echo -e "${BOLD}【HISTORY - 查看变更历史】${NC}"
    echo ""
    mvn liquibase:history \
        -Dliquibase.url="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
        -Dliquibase.username="$DB_USER" \
        -Dliquibase.password="$DB_PASS"
}

menu_update_sql() {
    echo ""
    echo -e "${BOLD}【UPDATE SQL - 预览 SQL】${NC}"
    echo ""
    echo -e "${YELLOW}即将执行的 SQL:${NC}"
    echo ""
    mvn liquibase:updateSQL \
        -Dliquibase.url="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
        -Dliquibase.username="$DB_USER" \
        -Dliquibase.password="$DB_PASS"
}

menu_rollback() {
    echo ""
    echo -e "${BOLD}【ROLLBACK - 回滚】${NC}"
    echo ""

    echo "  1. 回滚最近 N 个 changeset"
    echo "  2. 回滚到指定 tag (谨慎! 会回滚所有之后变更)"
    echo ""
    read -p "  选择操作 (1/2): " choice

    case $choice in
        1)
            read -p "  回滚数量: " count
            echo ""
            echo -e "${YELLOW}正在回滚最近 ${count} 个 changeset...${NC}"
            mvn liquibase:rollbackCount "$count" \
                -Dliquibase.url="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
                -Dliquibase.username="$DB_USER" \
                -Dliquibase.password="$DB_PASS"
            echo ""
            echo -e "${GREEN}✓ 回滚完成!${NC}"
            ;;
        2)
            read -p "  Tag 名称: " tag
            echo ""
            echo -e "${RED}⚠ 警告: 会回滚 tag 之后的所有变更!${NC}"
            read -p "  确认? (yes/no): " confirm
            if [ "$confirm" = "yes" ]; then
                echo -e "${YELLOW}正在回滚到 tag ${tag}...${NC}"
                mvn liquibase:rollback \
                    -Dliquibase.rollbackTag="$tag" \
                    -Dliquibase.url="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
                    -Dliquibase.username="$DB_USER" \
                    -Dliquibase.password="$DB_PASS"
                echo ""
                echo -e "${GREEN}✓ 回滚完成!${NC}"
            fi
            ;;
    esac
}

menu_diff() {
    echo ""
    echo -e "${BOLD}【DIFF - 生成 PRD 部署差异】${NC}"
    echo ""
    echo "  此功能用于生成 UAT 到 PRD 的差异脚本"
    echo ""

    read -p "  PRD 数据库主机: " prd_host
    prd_host=${prd_host:-"prd-mysql.internal"}
    read -p "  PRD 数据库端口: " prd_port
    prd_port=${prd_port:-3306}
    read -p "  PRD 数据库名: " prd_db
    prd_db=${prd_db:-"etl_db"}
    read -p "  PRD 用户名: " prd_user
    prd_user=${prd_user:-"liquibase"}
    read -s -p "  PRD 密码: " prd_pass
    echo ""

    output="changelogs/mapper/diff_to_prd_$(date +%Y%m%d_%H%M%S).sql"

    echo ""
    echo -e "${YELLOW}正在生成 diff...${NC}"
    mvn liquibase:diffChangeLog \
        -Dliquibase.url="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
        -Dliquibase.username="$DB_USER" \
        -Dliquibase.password="$DB_PASS" \
        -Dliquibase:referenceUrl="jdbc:mysql://${prd_host}:${prd_port}/${prd_db}" \
        -Dliquibase:referenceUsername="$prd_user" \
        -Dliquibase:referencePassword="$prd_pass" \
        -Dliquibase:diffChangeLogFile="$output"

    echo ""
    echo -e "${GREEN}✓ Diff 已生成: ${output}${NC}"
    echo ""
    echo -e "${YELLOW}请检查文件内容，确认无误后提交到仓库${NC}"
}

menu_uat_validate() {
    echo ""
    echo -e "${BOLD}【UAT VALIDATE - UAT 数据校验】${NC}"
    echo ""
    echo -e "${YELLOW}正在校验...${NC}"
    echo ""

    bash scripts/uat_validate.sh

    if [ $? -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✓ UAT 校验通过!${NC}"
    else
        echo ""
        echo -e "${RED}❌ UAT 校验失败!${NC}"
    fi
}

menu_conflicts() {
    echo ""
    echo -e "${BOLD}【CONFLICTS - 冲突检测】${NC}"
    echo ""
    echo -e "${YELLOW}正在检测冲突...${NC}"
    echo ""

    python3 scripts/check_conflicts.py

    if [ $? -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✓ 没有冲突${NC}"
    else
        echo ""
        echo -e "${RED}❌ 发现冲突，请先解决再提交${NC}"
    fi
}

menu_help() {
    echo ""
    echo -e "${BOLD}【帮助】${NC}"
    echo ""
    echo "  ${BOLD}开发阶段:${NC}"
    echo "    [N] NEW-CS    - 创建新的 changeset"
    echo "    [V] VALIDATE  - 验证 changeset 格式"
    echo "    [C] CONFLICTS - 检查是否有冲突"
    echo ""
    echo "  ${BOLD}UAT 阶段:${NC}"
    echo "    [U] UPDATE    - 部署到 UAT"
    echo "    [S] STATUS    - 查看待执行的变更"
    echo "    [1] UPDATE-SQL - 预览将执行的 SQL"
    echo "    [2] UAT-VALIDATE - 校验 UAT 数据"
    echo "    [H] HISTORY   - 查看变更历史"
    echo ""
    echo "  ${BOLD}PRD 准备:${NC}"
    echo "    [D] DIFF      - 生成 UAT 到 PRD 的差异"
    echo "    (生成的文件提交 PR，CI/CD 自动部署)"
    echo ""
    echo "  ${BOLD}回滚:${NC}"
    echo "    [R] ROLLBACK  - 回滚变更"
    echo ""
    echo "  ${BOLD}配置:${NC}"
    echo "    [O] DB CONFIG - 修改数据库配置"
    echo ""
    echo "  ${BOLD}提示:${NC}"
    echo "    PRD 部署由 CI/CD 自动完成"
    echo "    此工具只用于开发和 UAT 环境"
    echo ""
}

# ============================================================================
# 主菜单
# ============================================================================

show_menu() {
    print_header
    echo -e "${BOLD}请选择操作:${NC}"
    echo ""
    echo -e "${GREEN}━━━ 开发阶段 ━━━${NC}"
    echo -e "  ${GREEN}[N]${NC}  NEW-CS     - 创建 changeset"
    echo -e "  ${GREEN}[V]${NC}  VALIDATE   - 验证格式"
    echo -e "  ${GREEN}[C]${NC}  CONFLICTS  - 检查冲突"
    echo ""
    echo -e "${YELLOW}━━━ UAT 阶段 ━━━${NC}"
    echo -e "  ${YELLOW}[U]${NC}  UPDATE UAT - 部署到 UAT"
    echo -e "  ${YELLOW}[S]${NC}  STATUS     - 查看待执行"
    echo -e "  ${YELLOW}[1]${NC}  UPDATE-SQL - 预览 SQL"
    echo -e "  ${YELLOW}[2]${NC}  UAT-VALIDATE - 数据校验"
    echo -e "  ${YELLOW}[H]${NC}  HISTORY    - 查看历史"
    echo ""
    echo -e "${CYAN}━━━ PRD 准备 ━━━${NC}"
    echo -e "  ${CYAN}[D]${NC}  DIFF       - 生成差异脚本"
    echo ""
    echo -e "${RED}━━━ 回滚 ━━━${NC}"
    echo -e "  ${RED}[R]${NC}  ROLLBACK   - 回滚变更"
    echo ""
    echo -e "${BLUE}━━━ 配置 ━━━${NC}"
    echo -e "  ${BLUE}[O]${NC}  DB CONFIG  - 修改配置"
    echo ""
    echo -e "  ${GREEN}[?]${NC}  HELP       - 帮助"
    echo -e "  ${GREEN}[Q]${NC}  QUIT      - 退出"
    echo ""
}

# ============================================================================
# 主循环
# ============================================================================

main() {
    while true; do
        clear_screen
        show_menu

        echo -ne "${BOLD}请输入选项: ${NC}"
        read -n 1 choice
        echo ""

        case $choice in
            n|N)
                menu_new_changeset
                pause
                ;;
            v|V)
                menu_validate
                pause
                ;;
            c|C)
                menu_conflicts
                pause
                ;;
            u|U)
                menu_update_uat
                pause
                ;;
            s|S)
                menu_status
                pause
                ;;
            1)
                menu_update_sql
                pause
                ;;
            2)
                menu_uat_validate
                pause
                ;;
            h|H)
                menu_history
                pause
                ;;
            d|D)
                menu_diff
                pause
                ;;
            r|R)
                menu_rollback
                pause
                ;;
            o|O)
                read_db_config
                ;;
            \?)
                menu_help
                pause
                ;;
            q|Q)
                echo ""
                echo -e "${GREEN}再见!${NC}"
                echo ""
                break
                ;;
            *)
                echo ""
                echo -e "${RED}无效选项${NC}"
                sleep 1
                ;;
        esac
    done
}

# 检查前置条件
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}错误: Maven 未安装${NC}"
    exit 1
fi

if ! command -v mysql &> /dev/null; then
    echo -e "${RED}错误: MySQL 客户端未安装${NC}"
    exit 1
fi

# 启动
main
