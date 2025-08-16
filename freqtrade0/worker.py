"""
Main Freqtrade worker class.
"""

import asyncio
from datetime import datetime, timedelta
import time
import traceback
from collections.abc import Callable
from os import getpid
from typing import Any, Coroutine, overload

from numpy import isin
import sdnotify
from janus import T

from freqtrade import __version__, worker
from freqtrade0.freqtradebot import FreqtradeBot
from freqtrade.configuration import Configuration
from freqtrade.constants import PROCESS_THROTTLE_SECS, RETRY_TIMEOUT, Config
from freqtrade.enums import RPCMessageType, State
from freqtrade.exceptions import OperationalException, TemporaryError


logger = worker.logger


class Worker(worker.Worker):
    """
    Freqtradebot worker class
    """

    def __init__(self, args: dict[str, Any], config: Config | None = None,strategy:type|None=None) -> None:
        """
        Init all variables and objects the bot needs to work
        """
        logger.info(f"Starting worker {__version__}")

        self._args = args
        self._config = config
        self._init(False,strategy)

        self._heartbeat_msg: float = 0

        # Tell systemd that we completed initialization phase
        self._notify("READY=1")
        self.tasks= []

    def _init(self, reconfig: bool,strategy:type|None=None) -> None:
        """
        Also called from the _reconfigure() method (with reconfig=True).
        """
        if reconfig or self._config is None:
            # Load configuration
            self._config = Configuration(self._args, None).get_config()
        
        # Init the instance of the bot
        self.freqtrade = FreqtradeBot(self._config,strategy_type=strategy)

        internals_config = self._config.get("internals", {})
        self._throttle_secs = internals_config.get("process_throttle_secs", PROCESS_THROTTLE_SECS)
        self._heartbeat_interval = internals_config.get("heartbeat_interval", 60)

        self._sd_notify = (
            sdnotify.SystemdNotifier()
            if self._config.get("internals", {}).get("sd_notify", False)
            else None
        )

 

    
    
    def run(self) -> None:
        try:
          asyncio.run(self.gather(),debug=True)
        except asyncio.CancelledError as e:
            logger.error(e)
            pass
      
        

        
    async def gather(self):
        async def _work():
            state = None
            while True:
                try:
                    start = time.time()
                    state = self.worker(old_state=state)
                    if state == State.RELOAD_CONFIG:
                        self._reconfigure()
                    await self.sleep(start)
                except Exception as e:
                    # Log error and continue
                    await asyncio.sleep(1)  # Prevent tight-loop on critical failure

        async def refresh_trades():
            while True:
                try:
                    start = time.time()
                    await self.freqtrade.refresh_trades()
                    await self.sleep(start)
                except Exception as e:
                    # Log error and continue
                    await asyncio.sleep(1)

        async def refresh_ohlcv():
            while True:
                try:
                    start = time.time()
                    await self.freqtrade.refresh_ohlcv()
                    await self.sleep(start)
                except Exception as e:
                    # Log error and continue
                    await asyncio.sleep(1)

        tasks = [refresh_ohlcv(), self.process()]
        if self.freqtrade.use_public_trades:
            tasks.append(refresh_trades())
        await asyncio.gather(*tasks)
      
   
    
    async def async_reload_state(self,state:State, old_state:State|None=None):
        if state != old_state:
            if old_state != State.RELOAD_CONFIG:
                self.freqtrade.notify_status(f"{state.name.lower()}")

            logger.info(
                f"Changing state{f' from {old_state.name}' if old_state else ''} to: {state.name}"
            )
            if state in (State.RUNNING, State.PAUSED) and old_state not in (
                State.RUNNING,
                State.PAUSED,
            ):
                self.freqtrade.startup()

            if state == State.STOPPED:
                self.freqtrade.check_for_open_trades()

            # Reset heartbeat timestamp to log the heartbeat message at
            # first throttling iteration when the state changes
            self._heartbeat_msg = 0
        else:
            await self.async_process_loop_state(state)
    async def async_process_loop_state(self,state):
        if state in (State.RUNNING, State.PAUSED):
            state_str = "RUNNING" if state == State.RUNNING else "PAUSED"
            # Ping systemd watchdog before throttling
            self._notify(f"WATCHDOG=1\nSTATUS=State: {state_str}.")
            # Use an offset of 1s to ensure a new candle has been issued
            self.freqtrade.process()
        else:
            await self.async_stopstate(state)
    async def async_stopstate(self,state):
        if state == State.STOPPED:
            # Ping systemd watchdog before sleeping in the stopped state
            self._notify("WATCHDOG=1\nSTATUS=State: STOPPED.")
            self._process_stopped()
            await asyncio.sleep(self._throttle_secs)
    
    
   
   
    # async def async_process_running_callback(self,callback) -> None:
    #     try:
    #         await callback()
    #     except TemporaryError as error:
    #         logger.warning(f"Error: {error}, retrying in {RETRY_TIMEOUT} seconds...")
    #         await asyncio.sleep(RETRY_TIMEOUT)
    #     except OperationalException:
    #         tb = traceback.format_exc()
    #         hint = "Issue `/start` if you think it is safe to restart."

    #         self.freqtrade.notify_status(
    #             f"*OperationalException:*\n```\n{tb}```\n {hint}", msg_type=RPCMessageType.EXCEPTION
    #         )

    #         logger.exception("OperationalException. Stopping trader ...")
    #         self.freqtrade.state = State.STOPPED
    async def process(self):
        oldstate = None
        while True:
            start_time = time.time()
            state = self.freqtrade.state
            await self.async_reload_state(state,oldstate)
            oldstate = state
            await self.sleep(start_time)
      
        # self.register_looptask(self._process_loop_state)
        

    def worker(self, old_state: State | None) -> State:
        """
        The main routine that runs each throttling iteration and handles the states.
        :param old_state: the previous service state from the previous call
        :return: current service state
        """
        state = self.freqtrade.state

        # Log state transition
        if state != old_state:
            if old_state != State.RELOAD_CONFIG:
                self.freqtrade.notify_status(f"{state.name.lower()}")

            logger.info(
                f"Changing state{f' from {old_state.name}' if old_state else ''} to: {state.name}"
            )
            if state in (State.RUNNING, State.PAUSED) and old_state not in (
                State.RUNNING,
                State.PAUSED,
            ):
                self.freqtrade.startup()

            if state == State.STOPPED:
                self.freqtrade.check_for_open_trades()

            # Reset heartbeat timestamp to log the heartbeat message at
            # first throttling iteration when the state changes
            self._heartbeat_msg = 0

        if state == State.STOPPED:
            # Ping systemd watchdog before sleeping in the stopped state
            self._notify("WATCHDOG=1\nSTATUS=State: STOPPED.")
            self._process_stopped()

        elif state in (State.RUNNING, State.PAUSED):
            state_str = "RUNNING" if state == State.RUNNING else "PAUSED"
            # Ping systemd watchdog before throttling
            self._notify(f"WATCHDOG=1\nSTATUS=State: {state_str}.")

            # Use an offset of 1s to ensure a new candle has been issued
            self._process_running()
            

        if self._heartbeat_interval:
            now = time.time()
            if (now - self._heartbeat_msg) > self._heartbeat_interval:
                version = __version__
                strategy_version = self.freqtrade.strategy.version()
                if strategy_version is not None:
                    version += ", strategy_version: " + strategy_version
                logger.info(
                    f"Bot heartbeat. PID={getpid()}, version='{version}', state='{state.name}'"
                )
                self._heartbeat_msg = now

        return state
    def _gettime(self) -> float:
        return time.time()
   
    @overload
    async def sleep(
    self,
    start_time: datetime,  # 现在接收 datetime 对象
    *args,
    **kwargs):...
    @overload
    async def sleep(
        self,
        start_time:float,
        *args,
        **kwargs) :...
   
    async def sleep(
        self,
        start_time,
        *args,
        **kwargs
    ) :
        if isinstance(start_time,datetime):
            await self.sleep_datetime(start_time)
        elif isinstance(start_time,float):
            await self.sleep_s(start_time)
        else:
            raise TypeError(f"{start_time} is not a valid type")
    async def sleep_s(
        self,
        start_time:float,
        *args,
        **kwargs
    ) :
        """
        Throttles the given callable that it
        takes at least `min_secs` to finish execution.
        :param func: Any callable
        :param throttle_secs: throttling iteration execution time limit in seconds
        :param timeframe: ensure iteration is executed at the beginning of the next candle.
        :param timeframe_offset: offset in seconds to apply to the next candle time.
        :return: Any (result of execution of func)
        """
        sleep_duration =start_time + self._throttle_secs - time.time() 
        sleep_duration = max(sleep_duration, 0.0)
        if sleep_duration > 0:
            await asyncio.sleep(sleep_duration)
    async def sleep_datetime(
    self,
    start_time: datetime,  # 现在接收 datetime 对象
    *args,
    **kwargs
):
        """
        Throttles the given callable so that it takes at least `min_secs` to finish execution.
        :param start_time: Start time as a datetime object
        :param throttle_secs: Throttling iteration execution time limit in seconds
        :return: Any (result of execution of func)
        """
        current_time = datetime.now()
        
        # 确保 start_time 不早于当前时间（避免负的睡眠时间）
        adjusted_start_time = max(start_time, current_time)
        
        # 计算目标结束时间（开始时间 + throttle_secs）
        target_time = adjusted_start_time + timedelta(seconds=self._throttle_secs)
        
        # 计算需要睡眠的时间差
        sleep_duration = (target_time - current_time).total_seconds()
        sleep_duration = max(sleep_duration, 0.0)
        
        # 异步等待
        if sleep_duration > 0:
            await asyncio.sleep(sleep_duration)

  
   
       

    
