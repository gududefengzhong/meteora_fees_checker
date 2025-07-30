#!/usr/bin/env python3
"""
简单检查HTML中的国际化键
"""

import re

def extract_i18n_keys(file_path):
    """提取HTML中的所有国际化键"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 查找所有data-i18n属性
    i18n_pattern = r'data-i18n="([^"]+)"'
    i18n_keys = re.findall(i18n_pattern, content)
    
    # 查找所有data-i18n-placeholder属性
    placeholder_pattern = r'data-i18n-placeholder="([^"]+)"'
    placeholder_keys = re.findall(placeholder_pattern, content)
    
    return set(i18n_keys), set(placeholder_keys)

def check_hardcoded_text(file_path):
    """检查可能的硬编码文本"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 查找可能的硬编码中文文本
    chinese_pattern = r'[\u4e00-\u9fff]+'
    chinese_matches = re.findall(chinese_pattern, content)
    
    # 过滤掉在JavaScript注释或翻译对象中的中文
    hardcoded_chinese = []
    for match in chinese_matches:
        # 简单过滤，实际应该更精确
        if '翻译' not in match and '语言' not in match:
            hardcoded_chinese.append(match)
    
    return hardcoded_chinese

def main():
    """主函数"""
    print("🔍 检查HTML国际化情况")
    print("="*50)
    
    file_path = 'fees_checker.html'
    
    # 提取国际化键
    i18n_keys, placeholder_keys = extract_i18n_keys(file_path)
    
    print(f"📄 文件: {file_path}")
    print(f"🔑 data-i18n 键数量: {len(i18n_keys)}")
    print(f"🔑 data-i18n-placeholder 键数量: {len(placeholder_keys)}")
    
    print(f"\n📝 data-i18n 键列表:")
    for i, key in enumerate(sorted(i18n_keys), 1):
        print(f"   {i:2d}. {key}")
    
    if placeholder_keys:
        print(f"\n📝 data-i18n-placeholder 键列表:")
        for i, key in enumerate(sorted(placeholder_keys), 1):
            print(f"   {i:2d}. {key}")
    
    # 检查硬编码文本
    hardcoded = check_hardcoded_text(file_path)
    if hardcoded:
        print(f"\n⚠️  可能的硬编码文本:")
        for text in hardcoded[:10]:  # 只显示前10个
            print(f"   - {text}")
        if len(hardcoded) > 10:
            print(f"   ... 还有 {len(hardcoded) - 10} 个")
    else:
        print(f"\n✅ 未发现明显的硬编码文本")
    
    # 检查特定的联系和感谢部分
    print(f"\n🔍 检查联系和感谢部分:")
    
    # 检查是否包含联系开发者相关的键
    contact_keys = [key for key in i18n_keys if 'contact' in key.lower() or 'developer' in key.lower()]
    thanks_keys = [key for key in i18n_keys if 'thanks' in key.lower()]
    
    print(f"   联系相关键: {contact_keys}")
    print(f"   感谢相关键: {thanks_keys}")
    
    if contact_keys and thanks_keys:
        print(f"   ✅ 联系和感谢部分已国际化")
    else:
        print(f"   ❌ 联系和感谢部分可能未完全国际化")

if __name__ == "__main__":
    main()
