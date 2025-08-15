import os
import re
import shutil
from pathlib import Path
from typing import Literal


def curdir() -> Path:
    return Path.cwd()


def create_dir(*path_components: str | Path) -> Path:
    # 将传入的路径片段组合成一个Path对象
    p = Path(*path_components)
    if not p.exists():
        # 路径不存在，判断是否是文件路径
        if p.suffix:  # 如果路径包含后缀，则假设它是文件
            dir = p.parent
        else:  # 假设没有后缀的路径是文件夹路径
            dir = p
        if not dir.exists():
            dir.mkdir(parents=True, exist_ok=True)
    return p


def delete_dir(folder_path: Path | str):

    # 判断文件夹是否存在
    if os.path.exists(folder_path):
        # 删除文件夹及其所有内容
        shutil.rmtree(folder_path)


def convert_lowstr(s: str):
    return "".join(char.lower() for char in s if char.isalpha())


def isfile(*path_components: str | Path) -> bool:
    p = Path(*path_components)
    if p.exists():
        return p.is_file()
    else:
        if p.suffix:
            return True
        else:  # 假设没有后缀的路径是文件夹路径
            return False


def isdir(*path_components: str | Path) -> bool:
    p = Path(*path_components)
    if p.exists():
        return p.is_dir()
    else:
        # 路径不存在，判断是否是文件路径 如果路径包含后缀，则假设它是文件
        if p.suffix:
            return False
        else:  # 假设没有后缀的路径是文件夹路径
            return True


def get_paths(
    dir: Path,
    pattern: str = "**/*",
    include_hidden: bool = False,
    filedirtype: Literal["dir", "file", "all"] = "all",
):
    """
    Path.glob(pattern)
    解析相对于此路径的通配符 pattern,产生所有匹配的文件:

    >>>
    sorted(Path('.').glob('*.py'))
    [PosixPath('pathlib.py'), PosixPath('setup.py'), PosixPath('test_pathlib.py')]
    sorted(Path('.').glob('*/*.py'))
    [PosixPath('docs/conf.py')]
    pattern 的形式与 fnmatch 的相同，还增加了 "**" 表示 "此目录以及所有子目录，递归"。 换句话说，它启用递归通配:
    备注 在一个较大的目录树中使用 "**" 模式可能会消耗非常多的时间。
    >>>
    sorted(Path('.').glob('**/*.py'))
    [PosixPath('build/lib/pathlib.py'),
     PosixPath('docs/conf.py'),
     PosixPath('pathlib.py'),
     PosixPath('setup.py'),
     PosixPath('test_pathlib.py')]


    在 3.11 版本发生变更: 如果 pattern 以一个路径名称部分的分隔符结束 (sep 或 altsep) 则只返回目录
    使用glob.glob非递归查找所有文件
    '**/*' 表示在当前目录及其所有子目录中查找
     '*'	匹配0个或多个字符
     '**'	匹配所有文件、目录、子目录以及子目录中的文件(Python3.5新增)
     '?'	匹配一个字符
    '[]' '[!]'	匹配指定范围内的字符，比如：[0-9]匹配数字，[a-z]匹配小写字母
    """

    all_paths = dir.glob(pattern)
    filtered_paths = []

    for path in all_paths:
        # 过滤隐藏文件/目录
        if not include_hidden and any(part.startswith(".") for part in path.parts):
            continue

        match filedirtype:
            case "all":
                filtered_paths.append(path)
            case "dir":
                if path.is_dir():
                    filtered_paths.append(path)
            case "file":
                if path.is_file():
                    filtered_paths.append(path)
            case _:
                raise TypeError(
                    f"input type is '{filedirtype}', filedirtype must be 'all', 'dir' or 'file'."
                )

    return filtered_paths


def getsubdir(path: str | Path):
    path = Path(path)
    for entry in path.iterdir():
        if entry.is_dir():
            yield entry


def getsubfiles(path: str | Path):
    if isinstance(path, str):
        path = Path(path)
    for entry in path.iterdir():
        if entry.is_file():
            yield entry


def sanitize_filename(filename: str, repl="_"):
    """
    Args:
        file_path (str): 原始文件路径。
    Returns:
        str: 替换不允许字符后的文件路径。
    """

    pattern = r'[<>:"/\\|?*]'
    # 使用下划线替换匹配到的特殊字符
    new_filename = re.sub(pattern, repl, filename)
    # 返回替换后的文件名
    return new_filename


# # 文件夹监控线程
# def monitor_folder(path_to_watch: Path, envethandler):
#     '''
#     class FolderMonitorHandler(FileSystemEventHandler):

#     def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
#         """Called when a file or a directory is moved or renamed.

#         :param event:
#             Event representing file/directory movement.
#         :type event:
#             :class:`DirMovedEvent` or :class:`FileMovedEvent`
#         """

#     def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
#         """Called when a file or directory is created.

#         :param event:
#             Event representing file/directory creation.
#         :type event:
#             :class:`DirCreatedEvent` or :class:`FileCreatedEvent`
#         """

#     def on_deleted(self, event: DirDeletedEvent | FileDeletedEvent) -> None:
#         """Called when a file or directory is deleted.

#         :param event:
#             Event representing file/directory deletion.
#         :type event:
#             :class:`DirDeletedEvent` or :class:`FileDeletedEvent`
#         """

#     def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
#         """Called when a file or directory is modified.

#         :param event:
#             Event representing file/directory modification.
#         :type event:
#             :class:`DirModifiedEvent` or :class:`FileModifiedEvent`
#         """

#     def on_closed(self, event: FileClosedEvent) -> None:
#         """Called when a file opened for writing is closed.

#         :param event:
#             Event representing file closing.
#         :type event:
#             :class:`FileClosedEvent`
#         """

#     def on_closed_no_write(self, event: FileClosedNoWriteEvent) -> None:
#         """Called when a file opened for reading is closed.

#         :param event:
#             Event representing file closing.
#         :type event:
#             :class:`FileClosedNoWriteEvent`
#         """

#     def on_opened(self, event: FileOpenedEvent) -> None:
#         """Called when a file is opened.

#         :param event:
#             Event representing file opening.
#         :type event:
#             :class:`FileOpenedEvent`
#         """
#     '''
#     event_handler = envethandler()
#     observer = Observer()
#     observer.schedule(event_handler, path=str(path_to_watch), recursive=True)
#     observer.start()
#     print(f"正在监控文件夹: {path_to_watch}")
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         observer.stop()
#     observer.join()
