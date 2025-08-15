from typing import Any
from collections.abc import Iterator


class Enum:
    """
    类似 Enum 的自定义实现
    """

    # 类级别的字典，用于存储成员和值
    __kv__ = {}

    @classmethod
    def __init__(cls):
        cls._initialize()

    @classmethod
    def _initialize(cls):
        """
        初始化枚举成员字典（如果未初始化），并存储成员名称和值。
        """
        if not cls.__kv__ or len(cls.__kv__) <= 0:
            cls.__kv__ = dict[str, Any]()
            for attr, value in cls.__dict__.items():
                if not callable(value) and not attr.startswith("__"):
                    # 创建一个实例，绑定到枚举成员
                    cls.__kv__[attr] = value

    @classmethod
    def dict(cls) -> dict[str, Any]:
        """
        获取枚举成员的字典表示，初始化枚举成员（如果未初始化）。

        Returns:
            dict[str, Any]: 枚举成员的字典表示。
        """
        cls._initialize()  # 确保初始化枚举成员
        return cls.__kv__

    @classmethod
    def __iter__(cls):
        """
        返回枚举成员的项迭代器。

        Returns:
            Iterator[tuple[str, Any]]: 枚举成员的项迭代器，每个项为一个包含成员名和成员值的元组。
        ***注意: 不要省略括号class_name后面的 ()
            for k,v in class_name():
                print(k,v)

        """
        cls._initialize()  # 确保初始化枚举成员
        return iter(cls.__kv__.items())

    @classmethod
    def keys(cls) -> Iterator[str]:
        """
        返回枚举成员的键迭代器。

        Returns:
            Iterator[str]: 枚举成员的键迭代器。
        """
        cls._initialize()  # 确保初始化枚举成员
        return iter(cls.__kv__.keys())

    @classmethod
    def values(cls) -> Iterator[Any]:
        """
        返回枚举成员的键迭代器。

        Returns:
            Iterator[str]: 枚举成员的键迭代器。
        """
        cls._initialize()  # 确保初始化枚举成员
        return iter(cls.__kv__.values())
