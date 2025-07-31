#!/usr/bin/env python3
"""
测试累积数据功能
演示如何解决数据覆盖问题
"""

import json
import os

def simulate_multiple_runs():
    """模拟多次运行数据获取的场景"""

    print("🧪 模拟多次运行数据获取场景")
    print("="*60)

    # 模拟配置
    query_ids = [5556654]  # 只有一个查询ID，模拟你的情况
    data_dir = "meteora_data"

    print(f"📋 配置信息:")
    print(f"   查询ID: {query_ids}")
    print(f"   数据目录: {data_dir}")

    # 检查现有数据
    backup_file = os.path.join(data_dir, "full_wallet_data_backup.json")

    if os.path.exists(backup_file):
        with open(backup_file, 'r') as f:
            existing_data = json.load(f)

        print(f"\n📊 现有数据统计:")
        print(f"   钱包数: {len(existing_data)}")
        total_pairs = sum(len(pairs) for pairs in existing_data.values())
        print(f"   总交易对数: {total_pairs}")

        # 显示一些示例钱包
        multi_pair_wallets = [(w, p) for w, p in existing_data.items() if len(p) > 1]
        print(f"   多交易对钱包数: {len(multi_pair_wallets)}")

        if multi_pair_wallets:
            print(f"   示例多交易对钱包:")
            for i, (wallet, pairs) in enumerate(multi_pair_wallets[:3]):
                print(f"     {i+1}. {wallet[:8]}... -> {len(pairs)} 个交易对")
    else:
        print(f"\n📝 未找到现有数据文件")

    return data_dir

def demonstrate_solutions():
    """演示解决方案"""
    
    print(f"\n🔧 数据覆盖问题的解决方案:")
    print("="*60)
    
    print("1️⃣  **批次保护模式** (preserve_batches=True)")
    print("   - 使用时间戳命名批次文件")
    print("   - 避免覆盖历史批次数据")
    print("   - 批次文件命名: batch_20250730_143022_1_5556654")
    
    print("\n2️⃣  **数据累积模式** (accumulate_data=True)")
    print("   - 自动加载现有的钱包数据")
    print("   - 合并新数据和历史数据")
    print("   - 使用set去重，避免重复交易对")
    
    print("\n3️⃣  **推荐使用方式:**")
    print("   ```python")
    print("   fetcher.run_data_fetch(")
    print("       preserve_batches=True,    # 保护批次数据")
    print("       accumulate_data=True,     # 累积历史数据")
    print("       batch_delay=1.0          # 批次延迟")
    print("   )")
    print("   ```")

def show_usage_examples():
    """显示使用示例"""
    
    print(f"\n📚 使用示例:")
    print("="*60)
    
    print("🔄 **场景1: 每月运行一次，累积数据**")
    print("```python")
    print("# 第一次运行（1月数据）")
    print("query_ids = [5556654]  # 1月查询ID")
    print("fetcher = MeteoraDataFetcher(query_ids)")
    print("fetcher.run_data_fetch(accumulate_data=True)")
    print("")
    print("# 第二次运行（2月数据）")
    print("query_ids = [5556655]  # 2月查询ID")
    print("fetcher = MeteoraDataFetcher(query_ids)")
    print("fetcher.run_data_fetch(accumulate_data=True)  # 自动合并1月+2月数据")
    print("```")
    
    print("\n🔄 **场景2: 重新运行相同查询，不覆盖**")
    print("```python")
    print("# 多次运行相同查询ID")
    print("query_ids = [5556654]")
    print("fetcher = MeteoraDataFetcher(query_ids)")
    print("fetcher.run_data_fetch(")
    print("    preserve_batches=True,  # 保护历史批次")
    print("    accumulate_data=True    # 累积数据（去重）")
    print(")")
    print("```")
    
    print("\n🔄 **场景3: 强制覆盖（谨慎使用）**")
    print("```python")
    print("# 完全重新开始")
    print("fetcher.run_data_fetch(")
    print("    preserve_batches=False,  # 允许覆盖批次")
    print("    accumulate_data=False    # 不累积历史数据")
    print(")")
    print("```")

def main():
    """主函数"""

    # 1. 模拟当前情况
    data_dir = simulate_multiple_runs()

    # 2. 演示解决方案
    demonstrate_solutions()

    # 3. 显示使用示例
    show_usage_examples()

    # 4. 提供建议
    print(f"\n💡 针对你的情况的建议:")
    print("="*60)
    print("✅ 你目前只使用一个查询ID (5556654)")
    print("✅ 每次运行都会获取相同的数据")
    print("✅ 使用累积模式可以安全地重复运行")
    print("")
    print("🎯 **推荐配置:**")
    print("```python")
    print("query_ids = [5556654]  # 你的查询ID")
    print("fetcher = MeteoraDataFetcher(query_ids)")
    print("fetcher.run_data_fetch(")
    print("    preserve_batches=True,    # 保护批次数据")
    print("    accumulate_data=True,     # 累积模式（自动去重）")
    print("    batch_delay=1.0          # API延迟")
    print(")")
    print("```")

    print(f"\n🔮 **未来扩展:**")
    print("当你有多个月份的数据时:")
    print("```python")
    print("query_ids = [5556654, 5556655, 5556656]  # 多个月份")
    print("fetcher = MeteoraDataFetcher(query_ids)")
    print("fetcher.run_data_fetch(accumulate_data=True)")
    print("```")

    print(f"\n🎉 现在你可以安全地多次运行数据获取，不用担心覆盖问题了！")

if __name__ == "__main__":
    main()
