import json
import logging
import os
import time
from collections import defaultdict
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv
from dune_client.client import DuneClient

# 加载环境变量
load_dotenv()

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MeteoraDataFetcher:
    def __init__(self, query_ids: List[int] = None):
        """
        初始化Meteora数据获取器

        Args:
            query_ids: Dune查询ID列表，如果不提供则使用默认值
        """
        # 从环境变量获取API密钥
        dune_api_key = os.getenv('DUNE_API_KEY')
        if not dune_api_key:
            raise ValueError("请在.env文件中设置DUNE_API_KEY")

        self.dune = DuneClient(dune_api_key)
        self.query_ids = query_ids or [5556654]  # 默认查询ID，支持多个
        self.data_dir = "meteora_data"
        self.batch_data_dir = os.path.join(self.data_dir, "batches")

        # 创建数据目录
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.batch_data_dir, exist_ok=True)

    def save_raw_dune_data(self, df: pd.DataFrame, query_result):
        """保存原始Dune数据"""
        try:
            # 保存处理后的DataFrame为CSV格式
            csv_file = os.path.join(self.data_dir, "raw_dune_data.csv")
            df.to_csv(csv_file, index=False, encoding='utf-8')

            # 保存处理后的DataFrame为JSON格式
            json_file = os.path.join(self.data_dir, "raw_dune_data.json")
            df.to_json(json_file, orient='records', ensure_ascii=False, indent=2)

            # 保存完整的原始响应数据
            raw_response_file = os.path.join(self.data_dir, "dune_raw_response.json")
            raw_response_data = {
                "query_id": query_result.query_id,
                "result": {
                    "rows": query_result.result.rows,
                    "metadata": query_result.result.metadata.__dict__ if hasattr(query_result.result,
                                                                                 'metadata') else {}
                }
            }
            with open(raw_response_file, 'w', encoding='utf-8') as f:
                json.dump(raw_response_data, f, indent=2, ensure_ascii=False)

            # 保存数据摘要信息
            summary = {
                "query_id": query_result.query_id,
                "total_records": len(df),
                "unique_wallets": df['evt_tx_signer'].nunique(),
                "unique_pairs": df['lbPair'].nunique(),
                "columns": list(df.columns),
                "first_few_records": df.head(5).to_dict('records'),
                "data_types": df.dtypes.astype(str).to_dict(),
                "fetch_timestamp": pd.Timestamp.now().isoformat()
            }

            summary_file = os.path.join(self.data_dir, "dune_data_summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            logger.info(f"原始Dune数据已保存:")
            logger.info(f"  CSV文件: {csv_file}")
            logger.info(f"  JSON文件: {json_file}")
            logger.info(f"  原始响应: {raw_response_file}")
            logger.info(f"  摘要文件: {summary_file}")

        except Exception as e:
            logger.warning(f"保存原始Dune数据失败: {str(e)}")

    def fetch_single_batch(self, query_id: int, batch_name: str = None) -> pd.DataFrame:
        """
        获取单个批次的数据

        Args:
            query_id: 查询ID
            batch_name: 批次名称，用于保存文件

        Returns:
            DataFrame: 该批次的数据
        """
        if not batch_name:
            batch_name = f"batch_{query_id}"

        logger.info(f"获取批次 '{batch_name}' (查询ID: {query_id}) 的数据...")

        try:
            # 使用get_latest_result获取原始结果
            query_result = self.dune.get_latest_result(query_id)

            if not query_result or not query_result.result or not query_result.result.rows:
                logger.warning(f"批次 '{batch_name}' 未获取到数据或数据为空")
                return pd.DataFrame()

            # 从result.rows中提取数据
            rows_data = query_result.result.rows
            df = pd.DataFrame(rows_data)

            if df.empty:
                logger.warning(f"批次 '{batch_name}' 获取到的数据为空")
                return df

            # 验证必要列
            required_columns = ['evt_tx_signer', 'lbPair']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                logger.error(f"批次 '{batch_name}' 数据中缺少必要列: {missing_columns}")
                return pd.DataFrame()

            logger.info(f"批次 '{batch_name}' 成功获取 {len(df)} 条记录")

            # 保存批次数据
            self.save_batch_data(df, query_result, batch_name, query_id)

            return df

        except Exception as e:
            logger.error(f"获取批次 '{batch_name}' 数据失败: {str(e)}")
            return pd.DataFrame()

    def fetch_all_batches(self, delay_seconds: float = 1.0, preserve_batches: bool = True) -> List[pd.DataFrame]:
        """
        获取所有批次的数据

        Args:
            delay_seconds: 每个批次之间的延迟时间（秒）
            preserve_batches: 是否保留历史批次数据（避免覆盖）

        Returns:
            List[DataFrame]: 所有批次的数据列表
        """
        logger.info(f"开始获取 {len(self.query_ids)} 个批次的数据...")

        batch_dataframes = []

        # 生成时间戳用于批次命名
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        for i, query_id in enumerate(self.query_ids):
            if preserve_batches:
                # 使用时间戳避免覆盖历史数据
                batch_name = f"batch_{timestamp}_{i + 1}_{query_id}"
            else:
                # 传统命名方式（会覆盖）
                batch_name = f"batch_{i + 1}_{query_id}"

            logger.info(f"获取批次: {batch_name}")

            # 获取单个批次数据
            df = self.fetch_single_batch(query_id, batch_name)

            if not df.empty:
                batch_dataframes.append(df)

            # 添加延迟避免API限制（除了最后一个）
            if i < len(self.query_ids) - 1:
                logger.info(f"等待 {delay_seconds} 秒后获取下一批次...")
                time.sleep(delay_seconds)

        logger.info(f"完成所有批次数据获取，共获取 {len(batch_dataframes)} 个有效批次")
        return batch_dataframes

    def merge_batch_data(self, batch_dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """
        合并所有批次的数据

        Args:
            batch_dataframes: 批次数据列表

        Returns:
            DataFrame: 合并后的数据
        """
        if not batch_dataframes:
            logger.warning("没有可合并的批次数据")
            return pd.DataFrame()

        logger.info(f"开始合并 {len(batch_dataframes)} 个批次的数据...")

        # 合并所有DataFrame
        merged_df = pd.concat(batch_dataframes, ignore_index=True)

        # 去重（基于钱包地址和交易对）
        initial_count = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['evt_tx_signer', 'lbPair'], keep='first')
        final_count = len(merged_df)

        logger.info(f"数据合并完成：")
        logger.info(f"  合并前总记录数: {initial_count}")
        logger.info(f"  去重后总记录数: {final_count}")
        logger.info(f"  去除重复记录: {initial_count - final_count}")

        return merged_df

    def save_batch_data(self, df: pd.DataFrame, query_result, batch_name: str, query_id: int):
        """
        保存单个批次的数据

        Args:
            df: 批次数据DataFrame
            query_result: Dune查询结果
            batch_name: 批次名称
            query_id: 查询ID
        """
        try:
            batch_dir = os.path.join(self.batch_data_dir, batch_name)
            os.makedirs(batch_dir, exist_ok=True)

            # 保存CSV格式
            csv_file = os.path.join(batch_dir, f"{batch_name}.csv")
            df.to_csv(csv_file, index=False, encoding='utf-8')

            # 保存JSON格式
            json_file = os.path.join(batch_dir, f"{batch_name}.json")
            df.to_json(json_file, orient='records', ensure_ascii=False, indent=2)

            # 保存原始rows数据（只保存rows，不包含metadata）
            raw_rows_file = os.path.join(batch_dir, f"{batch_name}_raw_rows.json")
            raw_rows_data = {
                "batch_name": batch_name,
                "query_id": query_result.query_id,
                "rows": query_result.result.rows  # 只保存rows数据
            }
            with open(raw_rows_file, 'w', encoding='utf-8') as f:
                json.dump(raw_rows_data, f, indent=2, ensure_ascii=False)

            # 保存批次摘要
            summary = {
                "batch_name": batch_name,
                "query_id": query_id,
                "total_records": len(df),
                "unique_wallets": df['evt_tx_signer'].nunique(),
                "unique_pairs": df['lbPair'].nunique(),
                "columns": list(df.columns),
                "data_types": df.dtypes.astype(str).to_dict(),
                "fetch_timestamp": pd.Timestamp.now().isoformat()
            }

            summary_file = os.path.join(batch_dir, f"{batch_name}_summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            logger.info(f"批次 '{batch_name}' 数据已保存到: {batch_dir}")

        except Exception as e:
            logger.warning(f"保存批次 '{batch_name}' 数据失败: {str(e)}")

    def save_merged_data(self, merged_df: pd.DataFrame, batch_dataframes: List[pd.DataFrame]):
        """
        保存合并后的完整数据

        Args:
            merged_df: 合并后的数据
            batch_dataframes: 原始批次数据列表
        """
        try:
            # 保存合并后的CSV格式
            merged_csv_file = os.path.join(self.data_dir, "merged_dune_data.csv")
            merged_df.to_csv(merged_csv_file, index=False, encoding='utf-8')

            # 保存合并后的JSON格式
            merged_json_file = os.path.join(self.data_dir, "merged_dune_data.json")
            merged_df.to_json(merged_json_file, orient='records', ensure_ascii=False, indent=2)

            # 创建合并摘要信息
            merge_summary = {
                "total_batches": len(batch_dataframes),
                "batch_record_counts": [len(df) for df in batch_dataframes],
                "merged_total_records": len(merged_df),
                "unique_wallets": merged_df['evt_tx_signer'].nunique(),
                "unique_pairs": merged_df['lbPair'].nunique(),
                "columns": list(merged_df.columns),
                "data_types": merged_df.dtypes.astype(str).to_dict(),
                "merge_timestamp": pd.Timestamp.now().isoformat(),
                "blockchain": "Solana",
                "project": "Meteora DLMM"
            }

            # 保存合并摘要
            summary_file = os.path.join(self.data_dir, "merge_summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(merge_summary, f, indent=2, ensure_ascii=False)

            logger.info(f"合并数据已保存:")
            logger.info(f"  CSV文件: {merged_csv_file}")
            logger.info(f"  JSON文件: {merged_json_file}")
            logger.info(f"  摘要文件: {summary_file}")

        except Exception as e:
            logger.warning(f"保存合并数据失败: {str(e)}")

    def get_dune_data(self, delay_seconds: float = 1.0, preserve_batches: bool = True) -> pd.DataFrame:
        """
        从Dune获取所有批次数据并合并

        Args:
            delay_seconds: 每个批次之间的延迟时间（秒）
            preserve_batches: 是否保留历史批次数据（避免覆盖）

        Returns:
            DataFrame: 合并后的所有数据
        """
        logger.info(f"开始从Dune获取数据，共 {len(self.query_ids)} 个查询...")

        if preserve_batches:
            logger.info("🔒 启用批次保护模式，不会覆盖历史数据")
        else:
            logger.warning("⚠️  批次保护已关闭，可能会覆盖历史数据")

        try:
            # 获取所有批次数据
            batch_dataframes = self.fetch_all_batches(delay_seconds, preserve_batches)

            if not batch_dataframes:
                raise Exception("所有批次都未获取到有效数据")

            # 合并批次数据
            merged_df = self.merge_batch_data(batch_dataframes)

            if merged_df.empty:
                raise Exception("合并后的数据为空")

            # 保存合并后的完整数据
            self.save_merged_data(merged_df, batch_dataframes)

            return merged_df

        except Exception as e:
            logger.error(f"获取Dune数据失败: {str(e)}")
            raise

    def process_wallet_data(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """处理钱包数据，按钱包地址分组lbPair"""
        wallet_pairs = defaultdict(set)

        for _, row in df.iterrows():
            wallet = row['evt_tx_signer']
            lb_pair = row['lbPair']

            if pd.notna(wallet) and pd.notna(lb_pair):
                wallet_pairs[wallet].add(lb_pair)

        # 转换为列表
        result = {wallet: list(pairs) for wallet, pairs in wallet_pairs.items()}

        logger.info(f"处理完成：{len(result)} 个唯一钱包")
        total_pairs = sum(len(pairs) for pairs in result.values())
        logger.info(f"总计 {total_pairs} 个钱包-交易对组合")

        return result

    def create_wallet_index(self, wallet_data: Dict[str, List[str]], max_files: int = 16, max_wallets_per_file: int = 10000) -> Dict[str, str]:
        """
        创建钱包索引，用于快速查找
        优化的分组策略：控制文件数量，适合GitHub仓库

        Args:
            wallet_data: 钱包数据
            max_files: 最大文件数量
            max_wallets_per_file: 每个文件最大钱包数量
        """
        index = {}

        # 使用16进制字符作为第一级分组 (0-9, a-f)
        hex_chars = '0123456789abcdef'
        primary_groups = defaultdict(dict)

        # 第一级分组：按钱包地址第一个字符分组
        for wallet, pairs in wallet_data.items():
            first_char = wallet[0].lower()
            if first_char in hex_chars:
                primary_groups[first_char][wallet] = pairs
            else:
                # 非标准字符归入 'other' 组
                primary_groups['other'][wallet] = pairs

        logger.info(f"第一级分组完成，共 {len(primary_groups)} 个组")

        # 检查是否需要进一步细分
        final_groups = {}

        for group_key, group_data in primary_groups.items():
            if len(group_data) <= max_wallets_per_file:
                # 不需要细分
                final_groups[group_key] = group_data
                logger.info(f"组 '{group_key}': {len(group_data)} 个钱包，无需细分")
            else:
                # 需要细分：按第二个字符进一步分组
                logger.info(f"组 '{group_key}': {len(group_data)} 个钱包，需要细分")
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

                # 将细分后的组加入最终组
                for sub_key, sub_data in sub_groups.items():
                    final_groups[sub_key] = sub_data
                    logger.info(f"  子组 '{sub_key}': {len(sub_data)} 个钱包")

        # 创建文件并建立索引
        total_files = 0
        for group_key, group_data in final_groups.items():
            filename = f"wallets_{group_key}.json"
            filepath = os.path.join(self.data_dir, filename)

            # 创建优化的数据结构
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

            # 记录每个钱包属于哪个文件 - 确保所有钱包都被索引
            for wallet in group_data.keys():
                index[wallet] = filename
                logger.debug(f"索引钱包 {wallet} -> {filename}")

            total_files += 1

            # 计算文件大小
            file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
            logger.info(f"创建文件 '{filename}': {len(group_data)} 个钱包, {file_size:.2f} MB")

        # 验证索引完整性
        total_wallets_in_data = len(wallet_data)
        total_wallets_in_index = len(index)

        if total_wallets_in_data != total_wallets_in_index:
            logger.warning(f"索引不完整！原始数据: {total_wallets_in_data} 个钱包，索引: {total_wallets_in_index} 个钱包")

            # 找出缺失的钱包
            missing_wallets = set(wallet_data.keys()) - set(index.keys())
            if missing_wallets:
                logger.error(f"缺失的钱包地址: {list(missing_wallets)[:10]}...")  # 只显示前10个
        else:
            logger.info(f"✅ 索引完整性验证通过: {total_wallets_in_index} 个钱包")

        logger.info(f"索引创建完成，共创建 {total_files} 个文件")
        return index

    def save_optimized_data(self, wallet_data: Dict[str, List[str]], max_files: int = 16, max_wallets_per_file: int = 10000):
        """
        保存优化后的数据结构

        Args:
            wallet_data: 钱包数据
            max_files: 最大文件数量（用于GitHub仓库优化）
            max_wallets_per_file: 每个文件最大钱包数量
        """

        # 1. 创建钱包分组文件和索引
        wallet_index = self.create_wallet_index(wallet_data, max_files, max_wallets_per_file)

        # 2. 保存钱包索引（压缩格式，适合GitHub）
        index_file = os.path.join(self.data_dir, "wallet_index.json")
        with open(index_file, 'w', encoding='utf-8') as f:
            json.dump(wallet_index, f, separators=(',', ':'), ensure_ascii=False)

        # 3. 保存元数据
        total_files = len(set(wallet_index.values()))
        metadata = {
            "total_wallets": len(wallet_data),
            "total_pairs": sum(len(pairs) for pairs in wallet_data.values()),
            "total_files": total_files,
            "max_files_limit": max_files,
            "max_wallets_per_file": max_wallets_per_file,
            "last_updated": pd.Timestamp.now().isoformat(),
            "data_structure": "优化分组存储，适合GitHub仓库",
            "blockchain": "Solana",
            "project": "Meteora DLMM",
            "storage_strategy": "16进制字符分组，自动细分大文件",
            "github_optimized": True
        }

        metadata_file = os.path.join(self.data_dir, "metadata.json")
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, separators=(',', ':'), ensure_ascii=False)

        # 4. 创建钱包列表（压缩格式，用于前端搜索提示）
        wallet_list = sorted(list(wallet_data.keys()))  # 排序便于搜索
        wallet_list_file = os.path.join(self.data_dir, "wallet_list.json")
        with open(wallet_list_file, 'w', encoding='utf-8') as f:
            json.dump(wallet_list, f, separators=(',', ':'), ensure_ascii=False)

        # 5. 创建查询帮助文档
        query_help = {
            "how_to_query": "根据钱包地址查询对应的数据文件",
            "steps": [
                "1. 从 wallet_index.json 中查找钱包地址对应的文件名",
                "2. 加载对应的 wallets_*.json 文件",
                "3. 从文件的 wallets 字段中获取该钱包的 lbPair 列表"
            ],
            "example": {
                "wallet": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "step1": "查找 wallet_index.json['9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM']",
                "step2": "假设返回 'wallets_9.json'，则加载该文件",
                "step3": "获取 wallets_9.json['wallets']['9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM']"
            },
            "file_structure": "wallets_[group].json",
            "total_files": total_files
        }

        help_file = os.path.join(self.data_dir, "query_help.json")
        with open(help_file, 'w', encoding='utf-8') as f:
            json.dump(query_help, f, indent=2, ensure_ascii=False)

        logger.info("数据优化存储完成")
        logger.info(f"数据目录: {self.data_dir}")
        logger.info(f"总文件数: {total_files} (限制: {max_files})")
        logger.info(f"索引文件: {index_file}")
        logger.info(f"元数据文件: {metadata_file}")
        logger.info(f"查询帮助: {help_file}")
        logger.info("✅ GitHub仓库优化存储策略已应用")

    def rebuild_wallet_index(self):
        """
        重建钱包索引文件
        扫描所有 wallets_*.json 文件，重新生成完整的索引
        """
        logger.info("🔧 开始重建钱包索引...")

        index = {}
        total_wallets = 0

        # 扫描所有钱包数据文件
        import glob
        wallet_files = glob.glob(os.path.join(self.data_dir, "wallets_*.json"))

        for filepath in wallet_files:
            filename = os.path.basename(filepath)
            logger.info(f"处理文件: {filename}")

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)

                # 检查文件结构
                if 'wallets' in file_data:
                    wallets = file_data['wallets']
                else:
                    # 旧格式，直接是钱包数据
                    wallets = file_data

                # 为每个钱包建立索引
                for wallet in wallets.keys():
                    index[wallet] = filename
                    total_wallets += 1

                logger.info(f"  从 {filename} 索引了 {len(wallets)} 个钱包")

            except Exception as e:
                logger.error(f"处理文件 {filename} 时出错: {str(e)}")

        # 保存重建的索引
        index_file = os.path.join(self.data_dir, "wallet_index.json")
        with open(index_file, 'w', encoding='utf-8') as f:
            json.dump(index, f, separators=(',', ':'), ensure_ascii=False)

        logger.info(f"✅ 索引重建完成!")
        logger.info(f"   处理了 {len(wallet_files)} 个数据文件")
        logger.info(f"   索引了 {total_wallets} 个钱包地址")
        logger.info(f"   索引文件: {index_file}")

        return index

    def load_existing_wallet_data(self) -> Dict[str, List[str]]:
        """
        加载现有的钱包数据（如果存在）
        用于累积合并多次运行的数据
        """
        backup_file = os.path.join(self.data_dir, "full_wallet_data_backup.json")

        if os.path.exists(backup_file):
            try:
                with open(backup_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                logger.info(f"加载现有钱包数据: {len(existing_data)} 个钱包")
                return existing_data
            except Exception as e:
                logger.warning(f"加载现有数据失败: {str(e)}")
                return {}
        else:
            logger.info("未找到现有数据文件，将创建新的数据集")
            return {}

    def merge_wallet_data(self, existing_data: Dict[str, List[str]], new_data: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """
        合并现有数据和新数据

        Args:
            existing_data: 现有的钱包数据
            new_data: 新获取的钱包数据

        Returns:
            合并后的钱包数据
        """
        logger.info("开始合并钱包数据...")

        # 使用set来避免重复
        merged_data = defaultdict(set)

        # 添加现有数据
        for wallet, pairs in existing_data.items():
            merged_data[wallet].update(pairs)

        # 添加新数据
        for wallet, pairs in new_data.items():
            merged_data[wallet].update(pairs)

        # 转换回列表格式
        result = {wallet: list(pairs) for wallet, pairs in merged_data.items()}

        # 统计信息
        existing_wallets = len(existing_data)
        new_wallets = len(new_data)
        merged_wallets = len(result)

        existing_pairs = sum(len(pairs) for pairs in existing_data.values())
        new_pairs = sum(len(pairs) for pairs in new_data.values())
        merged_pairs = sum(len(pairs) for pairs in result.values())

        logger.info(f"数据合并完成:")
        logger.info(f"  现有钱包: {existing_wallets} -> 新钱包: {new_wallets} -> 合并后: {merged_wallets}")
        logger.info(f"  现有交易对: {existing_pairs} -> 新交易对: {new_pairs} -> 合并后: {merged_pairs}")
        logger.info(f"  新增钱包: {merged_wallets - existing_wallets}")
        logger.info(f"  新增交易对: {merged_pairs - existing_pairs}")

        return result

    def create_simple_lookup_api_data(self, wallet_data: Dict[str, List[str]]):
        """
        创建简单的查找API数据结构
        将所有数据存储在一个经过优化的JSON文件中
        """
        # 创建压缩的数据结构
        compressed_data = {}

        for wallet, pairs in wallet_data.items():
            # 只存储必要信息
            compressed_data[wallet] = pairs

        # 保存压缩数据
        api_data_file = os.path.join(self.data_dir, "wallet_pairs_api.json")
        with open(api_data_file, 'w', encoding='utf-8') as f:
            json.dump(compressed_data, f, separators=(',', ':'), ensure_ascii=False)

        logger.info(f"API数据文件已创建: {api_data_file}")

        # 计算文件大小
        file_size = os.path.getsize(api_data_file) / (1024 * 1024)  # MB
        logger.info(f"API数据文件大小: {file_size:.2f} MB")

        if file_size > 10:  # 如果文件大于10MB，建议使用分组存储
            logger.warning("数据文件较大，建议使用分组存储方案")

    def run_data_fetch(self, use_grouped_storage: bool = True, preserve_batches: bool = True,
                      batch_delay: float = 1.0, accumulate_data: bool = True):
        """
        运行完整的数据获取和存储流程

        Args:
            use_grouped_storage: 是否使用分组存储（推荐用于大数据量）
            preserve_batches: 是否保留历史批次数据（避免覆盖）
            batch_delay: 批次之间的延迟时间（秒）
            accumulate_data: 是否累积合并历史数据（解决覆盖问题）
        """
        try:
            # 1. 获取Dune数据
            df = self.get_dune_data(delay_seconds=batch_delay, preserve_batches=preserve_batches)

            # 2. 处理新获取的钱包数据
            new_wallet_data = self.process_wallet_data(df)

            if not new_wallet_data:
                raise Exception("未找到有效的钱包数据")

            # 3. 如果启用累积模式，合并历史数据
            if accumulate_data:
                logger.info("🔄 启用数据累积模式，合并历史数据...")
                existing_data = self.load_existing_wallet_data()
                wallet_data = self.merge_wallet_data(existing_data, new_wallet_data)
            else:
                logger.info("⚠️  数据累积已关闭，只使用当前批次数据")
                wallet_data = new_wallet_data

            # 4. 根据选择保存数据
            if use_grouped_storage:
                self.save_optimized_data(wallet_data)
            else:
                self.create_simple_lookup_api_data(wallet_data)

            # 4. 保存原始完整数据（备份）
            backup_file = os.path.join(self.data_dir, "full_wallet_data_backup.json")
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(wallet_data, f, indent=2, ensure_ascii=False)

            logger.info("数据获取和存储完成！")

            # 显示统计信息
            total_wallets = len(wallet_data)
            total_pairs = sum(len(pairs) for pairs in wallet_data.values())
            avg_pairs_per_wallet = total_pairs / total_wallets

            print(f"\n=== 数据统计 ===")
            print(f"总钱包数: {total_wallets:,}")
            print(f"总交易对数: {total_pairs:,}")
            print(f"平均每钱包交易对数: {avg_pairs_per_wallet:.2f}")
            print(f"数据存储目录: {self.data_dir}")
            print(f"区块链: Solana")
            print(f"项目: Meteora DLMM")

        except Exception as e:
            logger.error(f"数据获取失败: {str(e)}")
            raise


def main():
    """主函数"""
    print("🔧 Meteora 盈利查询器 - 数据获取工具")
    print("支持分批次拉取和合并Dune数据\n")

    # 默认配置 - 可以直接在这里修改
    DEFAULT_QUERY_IDS = [5556654]  # 在这里添加你的查询ID列表
    DEFAULT_BATCH_DELAY = 1.0  # 批次间延迟时间（秒）

    # 可以通过环境变量覆盖默认配置
    query_ids_env = os.getenv('DUNE_QUERY_IDS')
    if query_ids_env:
        try:
            # 支持逗号分隔的查询ID: "5556654,5556655,5556656"
            query_ids = [int(id.strip()) for id in query_ids_env.split(',')]
            print(f"✅ 从环境变量读取查询ID: {query_ids}")
        except ValueError:
            print(f"⚠️  环境变量DUNE_QUERY_IDS格式错误，使用默认值: {DEFAULT_QUERY_IDS}")
            query_ids = DEFAULT_QUERY_IDS
    else:
        query_ids = DEFAULT_QUERY_IDS
        print(f"✅ 使用默认查询ID: {query_ids}")

    # 批次延迟配置
    batch_delay_env = os.getenv('BATCH_DELAY')
    if batch_delay_env:
        try:
            batch_delay = float(batch_delay_env)
            print(f"✅ 从环境变量读取批次延迟: {batch_delay} 秒")
        except ValueError:
            print(f"⚠️  环境变量BATCH_DELAY格式错误，使用默认值: {DEFAULT_BATCH_DELAY} 秒")
            batch_delay = DEFAULT_BATCH_DELAY
    else:
        batch_delay = DEFAULT_BATCH_DELAY
        print(f"✅ 使用默认批次延迟: {batch_delay} 秒")

    try:
        # 创建数据获取器
        fetcher = MeteoraDataFetcher(query_ids)

        print(f"\n📋 配置摘要:")
        print(f"   查询ID列表: {query_ids}")
        print(f"   批次数量: {len(query_ids)}")
        print(f"   批次延迟: {batch_delay} 秒")

        print("\n" + "=" * 60)
        print("开始数据获取流程...")
        print("=" * 60)

        # 运行数据获取，使用分组存储
        fetcher.run_data_fetch(
            use_grouped_storage=True
        )

        print("\n🎉 所有操作完成！")
        print("\n📁 生成的文件:")
        print(f"   - 批次数据: {fetcher.batch_data_dir}/")
        print(f"   - 合并数据: {fetcher.data_dir}/merged_dune_data.*")
        print(f"   - 钱包数据: {fetcher.data_dir}/wallet_group_*.json")
        print(f"   - 摘要信息: {fetcher.data_dir}/merge_summary.json")

        print("\n✅ 现在可以使用前端页面进行Solana钱包盈利查询了。")
        print("⚠️  注意：请使用Solana钱包地址进行查询")

    except ValueError as e:
        print(f"❌ 配置错误: {str(e)}")
        print("请确保在项目根目录创建 .env 文件，并设置 DUNE_API_KEY=your_api_key_here")
    except KeyboardInterrupt:
        print(f"\n⏹️  用户中断操作")
    except Exception as e:
        print(f"❌ 执行失败: {str(e)}")


# 编程式使用示例
def custom_batch_example():
    """自定义批次示例"""
    print("📚 自定义批次使用示例:")

    query_ids = [5556654]
    fetcher = MeteoraDataFetcher(query_ids)

    # 方式2: 动态添加查询ID
    # fetcher.add_query_id(5556654)

    # 方式3: 自定义延迟时间
    # fetcher.run_data_fetch(batch_delay=3.0)

    return fetcher


if __name__ == "__main__":
    main()
