import asyncio

import pytest

from freqtrade0.system.asyncio import wait


@pytest.fixture(autouse=True)
def cleanup_event_loop():
    """Fixture to ensure clean event loop state before each test"""
    asyncio.set_event_loop(None)
    yield
    # Cleanup after test
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.close()

async def sample_coro():
    await asyncio.sleep(0.001)
    return "test_result"

async def error_coro():
    await asyncio.sleep(0.001)
    raise ValueError("test_error")
def test_try_finally():
    def try_finally():
        local_sort = 1
        try:
            print(f"执行 try {local_sort}块")
            return local_sort  # try 块中有 return 语句
        except:
            print("执行 except 块（本例不会触发）")
            return local_sort
        finally:
            local_sort+=1
            print(f"执行 finally 块（{local_sort}）")
    
    result = try_finally()+1
    print(f"函数返回结果：{result}")
    assert result ==2
def test_wait_without_existing_loop():
    """Test wait() when no event loop exists"""
    result = wait(sample_coro())
    assert result == "test_result"

def test_wait_with_existing_loop():
    """Test wait() when an event loop is already running"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    result = wait(sample_coro())
    assert result == "test_result"
    assert not loop.is_closed()
def test_top_existing_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async def test_task():
        await asyncio.sleep(0.001)
        result= wait(sample_coro())
        return result
    result = loop.run_until_complete(test_task())
    assert result == "test_result"
def test_top_asyncio_run():

    async def test_task():
        await asyncio.sleep(0.001)
        result= wait(sample_coro())
        return result
    result = asyncio.run(test_task())
    assert result == "test_result"

def test_wait_exception_handling():
    """Test wait() handles exceptions in coroutines"""
    with pytest.raises(ValueError) as exc_info:
        wait(error_coro())

    assert str(exc_info.value) == "test_error"