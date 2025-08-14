import asyncio
from asyncio import Future


def wait(coro):
    '''
    适用在同步函数中调用异步函数
    针对可能存在loop与不存在loop的场景做了处理
    '''
    loop = None
    new_loop = False
    result = None  # Initialize result to prevent unbound variable

    # Check if there's already a running loop
    try:
        loop = asyncio.get_running_loop()
        # If we have a running loop, use it directly
        result = loop.run_until_complete(coro)
        return result
    except RuntimeError:
        # If no running loop exists, create a new one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        new_loop = True

    try:
        result = loop.run_until_complete(coro)
    finally:
        if new_loop and loop and loop.is_running():
            loop.close()
    return result
