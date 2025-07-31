#!/usr/bin/env python3
"""
测试新的存储策略
演示如何优化钱包数据存储，适合GitHub仓库
"""

import json
import os
from collections import defaultdict
from typing import Dict, List
import pandas as pd

def simulate_wallet_data(num_wallets: int = 1000) -> Dict[str, List[str]]:
    """模拟钱包数据用于测试"""
    import random
    import string
    
    wallet_data = {}
    
    # 生成模拟的Solana钱包地址和lbPair
    for i in range(num_wallets):
        # 模拟Solana钱包地址（base58格式）
        wallet = ''.join(random.choices(string.ascii_letters + string.digits, k=44))
        
        # 模拟该钱包的lbPair列表
        num_pairs = random.randint(1, 10)
        pairs = []
        for j in range(num_pairs):
            pair = ''.join(random.choices(string.ascii_letters + string.digits, k=44))
            pairs.append(pair)
        
        wallet_data[wallet] = pairs
    
    return wallet_data

def test_new_storage_strategy():
    """测试新的存储策略"""
    print("🧪 测试新的存储策略...")
    
    # 1. 生成测试数据
    print("📊 生成测试数据...")
    wallet_data = simulate_wallet_data(5000)  # 5000个钱包用于测试
    
    print(f"   总钱包数: {len(wallet_data)}")
    total_pairs = sum(len(pairs) for pairs in wallet_data.values())
    print(f"   总交易对数: {total_pairs}")
    
    # 2. 应用新的存储策略
    print("\n💾 应用新的存储策略...")
    
    # 创建测试目录
    test_dir = "test_storage"
    os.makedirs(test_dir, exist_ok=True)
    
    # 使用新的分组算法
    index = create_optimized_wallet_index(wallet_data, test_dir)
    
    # 3. 分析结果
    print("\n📈 存储策略分析:")
    unique_files = set(index.values())
    print(f"   创建文件数: {len(unique_files)}")
    
    # 分析每个文件的大小
    file_stats = defaultdict(int)
    for wallet, filename in index.items():
        file_stats[filename] += 1
    
    print("   文件分布:")
    total_size = 0
    for filename, wallet_count in sorted(file_stats.items()):
        filepath = os.path.join(test_dir, filename)
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath) / 1024  # KB
            total_size += file_size
            print(f"     {filename}: {wallet_count} 个钱包, {file_size:.1f} KB")
    
    print(f"   总存储大小: {total_size:.1f} KB")
    print(f"   平均每文件: {total_size/len(unique_files):.1f} KB")
    
    # 4. 测试查询性能
    print("\n⚡ 查询性能测试:")
    test_wallets = list(wallet_data.keys())[:10]
    
    for wallet in test_wallets:
        filename = index.get(wallet)
        if filename:
            filepath = os.path.join(test_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
            
            pairs = file_data['wallets'].get(wallet, [])
            print(f"     钱包 {wallet[:8]}... -> {filename} ({len(pairs)} 个交易对)")
    
    print("\n✅ 存储策略测试完成!")
    return test_dir

def create_optimized_wallet_index(wallet_data: Dict[str, List[str]], data_dir: str, 
                                max_files: int = 16, max_wallets_per_file: int = 1000) -> Dict[str, str]:
    """
    创建优化的钱包索引（从meteora_data_fetcher.py复制的逻辑）
    """
    index = {}
    
    # 使用16进制字符作为第一级分组
    hex_chars = '0123456789abcdef'
    primary_groups = defaultdict(dict)
    
    # 第一级分组：按钱包地址第一个字符分组
    for wallet, pairs in wallet_data.items():
        first_char = wallet[0].lower()
        if first_char in hex_chars:
            primary_groups[first_char][wallet] = pairs
        else:
            primary_groups['other'][wallet] = pairs
    
    print(f"   第一级分组: {len(primary_groups)} 个组")
    
    # 检查是否需要进一步细分
    final_groups = {}
    
    for group_key, group_data in primary_groups.items():
        if len(group_data) <= max_wallets_per_file:
            final_groups[group_key] = group_data
            print(f"   组 '{group_key}': {len(group_data)} 个钱包 (无需细分)")
        else:
            print(f"   组 '{group_key}': {len(group_data)} 个钱包 (需要细分)")
            sub_groups = defaultdict(dict)
            
            for wallet, pairs in group_data.items():
                if len(wallet) > 1:
                    second_char = wallet[1].lower()
                    if second_char in hex_chars:
                        sub_key = f"{group_key}_{second_char}"
                    else:
                        sub_key = f"{group_key}_other"
                else:
                    sub_key = f"{group_key}_short"
                
                sub_groups[sub_key][wallet] = pairs
            
            for sub_key, sub_data in sub_groups.items():
                final_groups[sub_key] = sub_data
                print(f"     子组 '{sub_key}': {len(sub_data)} 个钱包")
    
    # 创建文件
    for group_key, group_data in final_groups.items():
        filename = f"wallets_{group_key}.json"
        filepath = os.path.join(data_dir, filename)
        
        optimized_data = {
            "group_info": {
                "group_key": group_key,
                "wallet_count": len(group_data),
                "total_pairs": sum(len(pairs) for pairs in group_data.values()),
                "created_at": pd.Timestamp.now().isoformat()
            },
            "wallets": group_data
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(optimized_data, f, separators=(',', ':'), ensure_ascii=False)
        
        for wallet in group_data.keys():
            index[wallet] = filename
    
    # 保存索引文件
    index_file = os.path.join(data_dir, "wallet_index.json")
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(index, f, separators=(',', ':'), ensure_ascii=False)
    
    return index

if __name__ == "__main__":
    test_dir = test_new_storage_strategy()
    
    print(f"\n📁 测试文件保存在: {test_dir}/")
    print("🔍 你可以查看生成的文件来了解新的存储结构")
    
    # 清理测试文件（可选）
    import shutil
    cleanup = input("\n🗑️  是否删除测试文件? (y/N): ").lower().strip()
    if cleanup == 'y':
        shutil.rmtree(test_dir)
        print("✅ 测试文件已清理")
    else:
        print("📁 测试文件保留，你可以手动查看和删除")
