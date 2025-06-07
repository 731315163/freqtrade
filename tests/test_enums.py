from math import exp
import pytest
from freqtrade.enums import MarketDirection, SignalDirection
from freqtrade0.enums import TradeDirection  # Update with actual module path


@pytest.mark.parametrize("direction,expected_direction", [
   (SignalDirection.SHORT, TradeDirection.SHORT),
    (SignalDirection.LONG, TradeDirection.LONG),
    (MarketDirection.SHORT, TradeDirection.SHORT),
    (MarketDirection.LONG, TradeDirection.LONG),
])
def test_signal_direction(direction,expected_direction):
    """Verify conversion from SignalDirection.SHORT"""
    assert TradeDirection.convert(direction) == expected_direction

# Parameterized test for string inputs
@pytest.mark.parametrize("input_str,expected", [
    ("long", TradeDirection.LONG),
    ("short", TradeDirection.SHORT),
    ("*", TradeDirection.BOTH),
    ("LONG", TradeDirection.NONE),  # Case-sensitive
    ("", TradeDirection.NONE),
    ("invalid", TradeDirection.NONE),
    ("123", TradeDirection.NONE),
])
def test_string_inputs(input_str, expected):
    """Test various string input scenarios"""
    assert TradeDirection.convert(input_str) == expected

# Test cases for non-string inputs
def test_none_input():
    """Verify NONE output for None input"""
    assert TradeDirection.convert(None) == TradeDirection.NONE

def test_other_types():
    """Verify NONE output for non-string/non-enum inputs"""
    assert TradeDirection.convert(123) == TradeDirection.NONE
    assert TradeDirection.convert(["long"]) == TradeDirection.NONE
    assert TradeDirection.convert({"key": "long"}) == TradeDirection.NONE

# Parameterized test for string inputs
@pytest.mark.parametrize("input_str,expected", [
    (2, TradeDirection.LONG),
    (1, TradeDirection.SHORT),
    (TradeDirection.LONG , TradeDirection.LONG),
    (TradeDirection.LONG|TradeDirection.SHORT, TradeDirection.BOTH),
    (TradeDirection.LONG|TradeDirection.SHORT|TradeDirection.NONE, TradeDirection.BOTH),
 
])
def test_equal_inputs(input_str, expected):
    """Test various string input scenarios"""
    assert input_str == expected

# Parameterized test for string inputs
@pytest.mark.parametrize("input_str,expected", [
    
    (TradeDirection.LONG, TradeDirection.BOTH),
    (TradeDirection.LONG, TradeDirection.NONE),  # Case-sensitive
    (TradeDirection.LONG, TradeDirection.SHORT),
  
])
def test_not_equal_inputs(input_str, expected):
    """Test various string input scenarios"""
    assert input_str != expected


@pytest.mark.parametrize("input_str,expected", [
    
    (TradeDirection.BOTH,TradeDirection.LONG),
    (TradeDirection.LONG, TradeDirection.SHORT),  # Case-sensitive
    (TradeDirection.SHORT,TradeDirection.NONE),
    (TradeDirection.LONG | TradeDirection.SHORT, TradeDirection.NONE)
  
])
def test_bigthan_inputs(input_str, expected):
    """Test various string input scenarios"""
    assert input_str > expected