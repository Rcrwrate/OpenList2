import os
import sys


def replace_string_in_file(file_path, old_str, new_str):
    """替换单个文件中的字符串"""
    try:
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()


        # 执行替换
        new_content = content.replace(old_str, new_str)


        # 如果内容有变化则写回文件
        if new_content != content:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(new_content)
            return True
        return False
    except UnicodeDecodeError:
        print(f"警告: 文件 '{file_path}' 可能不是文本文件，已跳过")
    except Exception as e:
        print(f"处理文件 '{file_path}' 时出错: {str(e)}")


def replace_string_in_directory(directory, old_str, new_str):
    """递归遍历目录并替换所有文件中的字符串"""
    changed_count = 0
    for root, _, files in os.walk(directory):
        for filename in files:
            file_path = os.path.join(root, filename)
            if replace_string_in_file(file_path, old_str, new_str):
                print(f"已修改: {file_path}")
                changed_count += 1
    return changed_count


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("用法: python replace_string.py <目录路径> <原字符串> <新字符串>")
        sys.exit(1)


    target_dir = sys.argv[1]
    old_string = sys.argv[2]
    new_string = sys.argv[3]


    if not os.path.isdir(target_dir):
        print(f"错误: 目录 '{target_dir}' 不存在")
        sys.exit(1)


    print(f"正在处理目录: {target_dir}")
    print(f"替换内容: '{old_string}' → '{new_string}'")


    changed_files = replace_string_in_directory(target_dir, old_string, new_string)
    print(f"\n完成! 共修改了 {changed_files} 个文件")