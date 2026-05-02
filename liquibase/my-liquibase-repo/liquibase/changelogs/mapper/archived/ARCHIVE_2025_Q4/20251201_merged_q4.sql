-- liquibase formatted sql
-- ============================================================================
-- Q4 2025 归档的 Mapper Changesets (已合并)
-- 构建时会被跳过,但在部署后会保留在数据库中
-- ============================================================================

-- changeset archived:2025Q4:001 author:team_a context:uat,prd
-- comment: [归档] 2025年Q4 合并的mapper变更

INSERT INTO s2t_mapping (map_name, map_version, maptype, view_name, map_detail, execution_order, is_active, description, created_by)
VALUES
    ('product_sync', 'v1', 'SQL-FILE', 'product_view', 'P->/data/hive/warehouse/product_db/tbl_products', 1, TRUE, '[归档]产品数据同步', 'team_a'),
    ('inventory_check', 'v1', 'SQL-SUBQUERY', 'inv_view', 'I->SELECT * FROM inventory_db.stock WHERE quantity > 0', 1, TRUE, '[归档]库存检查', 'team_b');

-- changeset archived:2025Q4:002 author:team_c context:uat,prd
-- comment: [归档] 2025年10月 mapper 更新

INSERT INTO s2t_mapping (map_name, map_version, maptype, view_name, map_detail, execution_order, is_active, description, created_by)
VALUES
    ('customer_seg', 'v1', 'SQL-SUBQUERY', 'seg_view', 'S->SELECT customer_id, seg_type FROM customer_seg WHERE active = 1', 1, TRUE, '[归档]客户分群', 'team_c');
