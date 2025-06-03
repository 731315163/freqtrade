from pathlib import Path
import re

def replace_import_statements(source_dir:Path, target_dir:Path, old_module:str, new_module:str,filenames:list[str]):
    """
    替换指定目录下所有.py文件中的模块导入语句，使用Pathlib实现
    
    Args:
        source_dir: 源文件根目录
        target_dir: 目标文件根目录
        old_module: 需要替换的旧模块名
        new_module: 新模块名
    """
   
    
    # 确保目标目录存在
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # 构建正则表达式模式
    pattern = re.compile(
        rf'(from\s+){re.escape(old_module)}(\.exchange\s+import)', 
        re.MULTILINE
    )
    replacement = fr'\g<1>{new_module}\g<2>'
    
    # 使用Pathlib递归遍历所有.py文件
    if len(filenames)>0:
        files = [ source_dir/name for name in filenames ]
    else:
        raise ValueError("请指定文件名",source_dir.rglob('*.py'))
    for source_file in files:
        try:
            # 读取文件内容
            content = source_file.read_text(encoding='utf-8')
            
            # 替换内容
            new_content = pattern.sub(replacement, content)
            
            # 保留子目录结构
            rel_path = source_file.relative_to(source_dir)
            target_file = target_dir / rel_path
            
            # 创建目标文件父目录
            target_file.parent.mkdir(parents=True, exist_ok=True)
            
            # 写入新文件
            target_file.write_text(new_content, encoding='utf-8')
            
            print(f"已修改文件: {source_file} -> {target_file}")
            
        except Exception as e:
            print(f"处理文件失败 {source_file}: {str(e)}")

if __name__ == '__main__':
    # 示例用法
    source_dir = Path.cwd()/"freqtrade"/"exchange"  # 替换为你的项目根目录
    target_dir = Path.cwd()/"freqtrade0"/"exchange"    # 替换为目标目录
    old_module = 'freqtrade'
    new_module = 'freqtrade0'
    filenames=["binance.py","bybit.py","okx.py"]
    replace_import_statements(source_dir, target_dir, old_module, new_module)