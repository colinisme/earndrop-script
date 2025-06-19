# earndrop-script

0. 使用 init_data 创建测试数据
1. 使用 generate_merkle_tree 生成merkle-tree、merkle_proofs，保存到文件中
2. 使用validate_merkle_proofs 验证merkle_proofs的正确性
3. 使用 import_to_db 导入到数据库中
4. 压测接口